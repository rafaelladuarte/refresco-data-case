import pandas as pd
import s3fs
import logging
import re

from pipeline.settings import s3_storage_options

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Padrões de ID válidos — ajuste as regexes conforme o seu padrão de negócio
# ---------------------------------------------------------------------------
PATTERN_ID_CLIENTE = re.compile(r'^C\d{4}$', re.IGNORECASE)   # ex: C0001 – C9999
PATTERN_ID_PRODUTO = re.compile(r'^P\d{3}$', re.IGNORECASE)   # ex: P001 – P999


def parse_mixed_dates(date_series):
    """Trata datas em formatos mistos (ISO e BR) para evitar NaT."""
    dt1 = pd.to_datetime(date_series, format="%Y-%m-%d", errors="coerce")
    dt2 = pd.to_datetime(date_series, format="%d/%m/%Y", errors="coerce")
    return dt1.combine_first(dt2)


def normalize_id(series: pd.Series) -> pd.Series:
    """Remove espaços e converte None/NaN para string vazia para facilitar validação."""
    return series.fillna("").astype(str).str.strip()


def validate_referential_integrity(
    df_vendas: pd.DataFrame,
    df_clientes: pd.DataFrame,
    df_produtos: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Valida integridade referencial de vendas contra os cadastros de clientes e produtos.

    Regras aplicadas (em ordem):
      1. id_cliente e id_produto não podem ser vazios ou brancos.
      2. id_cliente deve casar com o padrão PATTERN_ID_CLIENTE.
      3. id_produto deve casar com o padrão PATTERN_ID_PRODUTO.
      4. id_cliente deve existir em df_clientes.
      5. id_produto deve existir em df_produtos.

    Retorna:
      df_ok       — registros que passaram em todas as regras (→ Silver)
      df_quarentena — registros rejeitados, com coluna `motivo_rejeicao` (→ Quarentena)
    """
    df = df_vendas.copy()
    df["id_cliente"] = normalize_id(df["id_cliente"])
    df["id_produto"] = normalize_id(df["id_produto"])

    ids_clientes_validos = set(normalize_id(df_clientes["id_cliente"]))
    ids_produtos_validos = set(normalize_id(df_produtos["id_produto"]))

    motivos = []

    # Regra 1 — IDs em branco
    mask_cliente_vazio = df["id_cliente"] == ""
    mask_produto_vazio = df["id_produto"] == ""

    # Regra 2/3 — Formato de ID inválido
    mask_cliente_formato = ~df["id_cliente"].str.match(PATTERN_ID_CLIENTE) & ~mask_cliente_vazio
    mask_produto_formato = ~df["id_produto"].str.match(PATTERN_ID_PRODUTO) & ~mask_produto_vazio

    # Regra 4/5 — ID não encontrado no cadastro
    mask_cliente_orphao = (~df["id_cliente"].isin(ids_clientes_validos)) & ~mask_cliente_vazio
    mask_produto_orphao = (~df["id_produto"].isin(ids_produtos_validos)) & ~mask_produto_vazio

    # Combina todas as máscaras de rejeição
    mask_rejeicao = (
        mask_cliente_vazio | mask_produto_vazio
        | mask_cliente_formato | mask_produto_formato
        | mask_cliente_orphao | mask_produto_orphao
    )

    # Monta coluna descritiva de motivo para auditoria
    def montar_motivo(row):
        razoes = []
        if row["id_cliente"] == "":
            razoes.append("id_cliente vazio")
        elif not PATTERN_ID_CLIENTE.match(row["id_cliente"]):
            razoes.append(f"id_cliente formato inválido: '{row['id_cliente']}'")
        elif row["id_cliente"] not in ids_clientes_validos:
            razoes.append(f"id_cliente sem cadastro: '{row['id_cliente']}'")

        if row["id_produto"] == "":
            razoes.append("id_produto vazio")
        elif not PATTERN_ID_PRODUTO.match(row["id_produto"]):
            razoes.append(f"id_produto formato inválido: '{row['id_produto']}'")
        elif row["id_produto"] not in ids_produtos_validos:
            razoes.append(f"id_produto sem cadastro: '{row['id_produto']}'")

        return " | ".join(razoes)

    df_quarentena = df[mask_rejeicao].copy()
    df_quarentena["motivo_rejeicao"] = df_quarentena.apply(montar_motivo, axis=1)

    df_ok = df[~mask_rejeicao].copy()
    return df_ok, df_quarentena


def main():
    logger.info("Iniciando processamento Silver para Vendas")

    storage_options = s3_storage_options()

    try:
        fs = s3fs.S3FileSystem(**storage_options)

        # ------------------------------------------------------------------
        # 1. Lê vendas da Bronze
        # ------------------------------------------------------------------
        arquivos = fs.glob("bronze/vendas/**/*.csv")
        if not arquivos:
            logger.warning("Nenhum arquivo encontrado em bronze/vendas/")
            return

        logger.info(f"Encontrados {len(arquivos)} arquivos na Bronze. Lendo dados...")
        df = pd.concat(
            [pd.read_csv(f"s3://{a}", storage_options=storage_options) for a in arquivos],
            ignore_index=True,
        )
        logger.info(f"Linhas lidas da Bronze: {len(df)}")

        # ------------------------------------------------------------------
        # 2. Normalização de strings antes de qualquer validação
        #    Garante que espaços em branco não passem despercebidos
        # ------------------------------------------------------------------
        for col in ["id_venda", "id_cliente", "id_produto", "canal_venda", "representante"]:
            df[col] = df[col].fillna("").astype(str).str.strip()

        # ------------------------------------------------------------------
        # 3. Transformações de negócio
        # ------------------------------------------------------------------
        df["data_emissao"] = parse_mixed_dates(df["data_emissao"])
        df["ano"] = df["data_emissao"].dt.year
        df["mes"] = df["data_emissao"].dt.month

        for col in ["qtd_caixas", "preco_unitario", "desconto_pct"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        receita_bruta = df["qtd_caixas"] * df["preco_unitario"]
        df["receita_liquida"] = receita_bruta * (1 - df["desconto_pct"].fillna(0))

        df["_ingested_at"] = pd.to_datetime(df["_ingested_at"], errors="coerce")
        df = df.sort_values("_ingested_at").drop_duplicates(subset=["id_venda"], keep="last")

        # Remove linhas sem data válida ou id_venda — não são recuperáveis
        df = df.dropna(subset=["data_emissao", "ano", "mes"])
        df = df[df["id_venda"] != ""]
        logger.info(f"Após limpeza básica: {len(df)} linhas")

        # ------------------------------------------------------------------
        # 4. Carrega cadastros de referência da Silver para lookup
        #    (assumindo que clean_clientes e clean_produtos já rodaram antes)
        # ------------------------------------------------------------------
        logger.info("Carregando cadastros de referência da Silver para validação...")
        df_clientes = pd.read_parquet("s3://silver/clientes/", storage_options=storage_options)
        df_produtos = pd.read_parquet(
            "s3://silver/produtos/produtos_clean.parquet", storage_options=storage_options
        )

        # ------------------------------------------------------------------
        # 5. Validação de integridade referencial
        # ------------------------------------------------------------------
        df_ok, df_quarentena = validate_referential_integrity(df, df_clientes, df_produtos)

        logger.info(f"Registros aprovados (→ Silver): {len(df_ok)}")
        logger.info(f"Registros rejeitados (→ Quarentena): {len(df_quarentena)}")

        if not df_quarentena.empty:
            # Log dos motivos para visibilidade imediata no Airflow
            for motivo, grupo in df_quarentena.groupby("motivo_rejeicao"):
                logger.warning(f"  [{len(grupo)} registros] {motivo}")

            # Persiste quarentena para auditoria e reprocessamento manual
            quarentena_path = "s3://silver/_quarentena/vendas/"
            if fs.exists(quarentena_path):
                fs.rm(quarentena_path, recursive=True)
            df_quarentena.to_parquet(
                path=quarentena_path,
                storage_options=storage_options,
                index=False,
                compression="snappy",
            )
            logger.warning(f"[QUARENTENA] {len(df_quarentena)} registros salvos em {quarentena_path}")

        # ------------------------------------------------------------------
        # 6. Salva apenas registros íntegros na Silver
        # ------------------------------------------------------------------
        target_path_full = "s3://silver/vendas/"
        if fs.exists(target_path_full):
            logger.info(f"Limpando diretório existente: {target_path_full}")
            fs.rm(target_path_full, recursive=True)

        df_ok.to_parquet(
            path=target_path_full,
            partition_cols=["ano", "mes"],
            storage_options=storage_options,
            index=False,
            compression="snappy",
        )
        logger.info("[OK] Tabela de Vendas processada e salva com sucesso na Silver!")

    except Exception as e:
        logger.error(f"[ERRO] Erro no processamento de Vendas: {str(e)}")
        raise


if __name__ == "__main__":
    main()