import pandas as pd

from pipeline.common.logging import get_logger
from pipeline.common.storage import (
    get_s3_client,
    read_bronze,
    read_silver,
    write_quarantine,
    write_silver,
)
from pipeline.common.validators import (
    PATTERN_ID_CLIENTE,
    PATTERN_ID_PRODUTO,
    clean_text,
    normalize_id,
)

logger = get_logger(__name__)

# Formatos de data aceitos em data_emissao. São mutuamente exclusivos (uma string
# casa com no máximo um), então a ordem não altera o resultado.
DATE_FORMATS = ("%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y", "%d-%m-%Y")


def parse_mixed_dates(date_series: pd.Series) -> pd.Series:
    """Converte datas em formatos mistos (ISO e variações BR); inválidas viram NaT."""
    result = pd.Series(pd.NaT, index=date_series.index, dtype="datetime64[ns]")
    for fmt in DATE_FORMATS:
        result = result.combine_first(pd.to_datetime(date_series, format=fmt, errors="coerce"))
    return result


def validate_vendas(
    df_vendas: pd.DataFrame,
    df_clientes: pd.DataFrame,
    df_produtos: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Valida vendas: campos obrigatórios, medidas e integridade referencial.

    Espera `data_emissao` já convertida (NaT = data inválida) e a coluna
    `data_emissao_original` com o texto de origem, usado na mensagem de rejeição.

    Regras (um registro pode violar várias; todas entram no motivo):
      - id_venda não pode ser vazio
      - data_emissao deve estar em um dos DATE_FORMATS e ser uma data válida
      - qtd_caixas > 0
      - preco_unitario > 0
      - 0 <= desconto_pct < 1
      - id_cliente / id_produto: não vazios, no formato esperado e existentes no cadastro

    Retorna:
      df_ok         — registros que passaram em todas as regras (→ Silver)
      df_quarentena — registros rejeitados, com coluna `motivo_rejeicao` (→ Quarentena)
    """
    df = df_vendas.copy()
    df["id_cliente"] = normalize_id(df["id_cliente"])
    df["id_produto"] = normalize_id(df["id_produto"])

    ids_clientes_validos = set(normalize_id(df_clientes["id_cliente"]))
    ids_produtos_validos = set(normalize_id(df_produtos["id_produto"]))

    def fmt(col):
        return "'" + df[col].astype("string").fillna("<nulo>") + "'"

    cliente_vazio = df["id_cliente"] == ""
    cliente_formato_ok = df["id_cliente"].str.match(PATTERN_ID_CLIENTE)
    produto_vazio = df["id_produto"] == ""
    produto_formato_ok = df["id_produto"].str.match(PATTERN_ID_PRODUTO)

    # (máscara de violação, mensagem) — a mensagem pode ser fixa ou por linha
    regras = [
        (df["id_venda"] == "", "id_venda vazio"),
        (df["data_emissao"].isna(), "data_emissao inválida: " + fmt("data_emissao_original")),
        (~(df["qtd_caixas"] > 0), "qtd_caixas deve ser > 0: " + fmt("qtd_caixas")),
        (~(df["preco_unitario"] > 0), "preco_unitario deve ser > 0: " + fmt("preco_unitario")),
        (
            ~df["desconto_pct"].between(0, 1, inclusive="left"),
            "desconto_pct fora de [0, 1): " + fmt("desconto_pct"),
        ),
        (cliente_vazio, "id_cliente vazio"),
        (~cliente_vazio & ~cliente_formato_ok, "id_cliente formato inválido: " + fmt("id_cliente")),
        (
            ~cliente_vazio & cliente_formato_ok & ~df["id_cliente"].isin(ids_clientes_validos),
            "id_cliente sem cadastro: " + fmt("id_cliente"),
        ),
        (produto_vazio, "id_produto vazio"),
        (~produto_vazio & ~produto_formato_ok, "id_produto formato inválido: " + fmt("id_produto")),
        (
            ~produto_vazio & produto_formato_ok & ~df["id_produto"].isin(ids_produtos_validos),
            "id_produto sem cadastro: " + fmt("id_produto"),
        ),
    ]

    if df.empty:
        return df.drop(columns="data_emissao_original"), df.assign(motivo_rejeicao=pd.Series(dtype="string"))

    mensagens = pd.DataFrame(index=df.index)
    for i, (mask, msg) in enumerate(regras):
        mask = mask.fillna(True).astype(bool)
        mensagens[i] = pd.Series(msg, index=df.index).where(mask)
    motivo = mensagens.apply(lambda row: " | ".join(row.dropna()), axis=1)
    mask_rejeicao = motivo != ""

    df_quarentena = df[mask_rejeicao].copy()
    df_quarentena["motivo_rejeicao"] = motivo[mask_rejeicao]
    df_ok = df[~mask_rejeicao].drop(columns="data_emissao_original")
    return df_ok, df_quarentena


def main():
    logger.info("Iniciando processamento Silver para Vendas")

    try:
        fs = get_s3_client()

        # ------------------------------------------------------------------
        # 1. Lê vendas da Bronze
        # ------------------------------------------------------------------
        df = read_bronze(fs, "vendas")
        if df is None:
            return
        logger.info(f"Linhas lidas da Bronze: {len(df)}")

        # ------------------------------------------------------------------
        # 2. Normalização de strings antes de qualquer validação
        #    Garante que espaços em branco não passem despercebidos
        # ------------------------------------------------------------------
        # IDs: vazio ("") em vez de nulo, pois as regras de validação comparam com string vazia
        for col in ["id_venda", "id_cliente", "id_produto"]:
            df[col] = normalize_id(df[col])
        # Atributos descritivos: nulo permanece nulo (sem virar "nan" ou "")
        for col in ["canal_venda", "representante"]:
            df[col] = clean_text(df[col])

        # ------------------------------------------------------------------
        # 3. Transformações de negócio
        # ------------------------------------------------------------------
        df["data_emissao_original"] = df["data_emissao"].astype("string")
        df["data_emissao"] = parse_mixed_dates(df["data_emissao"])

        for col in ["qtd_caixas", "preco_unitario", "desconto_pct"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        receita_bruta = df["qtd_caixas"] * df["preco_unitario"]
        df["receita_liquida"] = receita_bruta * (1 - df["desconto_pct"].fillna(0))

        df["_ingested_at"] = pd.to_datetime(df["_ingested_at"], errors="coerce")
        # Ordenação estável: em empate de _ingested_at o resultado é determinístico
        df = df.sort_values("_ingested_at", kind="mergesort").drop_duplicates(
            subset=["id_venda"], keep="last"
        )
        logger.info(f"Após deduplicação por id_venda: {len(df)} linhas")

        # ------------------------------------------------------------------
        # 4. Carrega cadastros de referência da Silver para lookup
        #    (assumindo que clean_clientes e clean_produtos já rodaram antes)
        # ------------------------------------------------------------------
        logger.info("Carregando cadastros de referência da Silver para validação...")
        df_clientes = read_silver("s3://silver/clientes/")
        df_produtos = read_silver("s3://silver/produtos/produtos_clean.parquet")

        # ------------------------------------------------------------------
        # 5. Validação: obrigatórios, medidas e integridade referencial.
        #    Nada é descartado em silêncio: toda rejeição vai para a quarentena.
        # ------------------------------------------------------------------
        df_ok, df_quarentena = validate_vendas(df, df_clientes, df_produtos)

        # Colunas de partição calculadas só para aprovados (data sempre válida).
        # int64 nativo: Int64 (nullable) em partition_cols não é relido pelo pyarrow 15.
        df_ok["ano"] = df_ok["data_emissao"].dt.year.astype("int64")
        df_ok["mes"] = df_ok["data_emissao"].dt.month.astype("int64")

        logger.info(f"Registros aprovados (→ Silver): {len(df_ok)}")
        logger.info(f"Registros rejeitados (→ Quarentena): {len(df_quarentena)}")

        if not df_quarentena.empty:
            # Log por tipo de regra (sem o valor específico) para visibilidade no Airflow
            regras = (
                df_quarentena["motivo_rejeicao"].str.split(" | ", regex=False).explode().str.split(":").str[0]
            )
            for regra, qtd in regras.value_counts().items():
                logger.warning(f"  [{qtd} registros] {regra}")

        # Persiste a quarentena para auditoria e reprocessamento manual. A da execução
        # anterior é removida mesmo que a atual venha vazia.
        quarentena_path = write_quarantine(fs, df_quarentena, "vendas")
        if not df_quarentena.empty:
            logger.warning(f"[QUARENTENA] {len(df_quarentena)} registros salvos em {quarentena_path}")

        # ------------------------------------------------------------------
        # 6. Salva apenas registros íntegros na Silver
        # ------------------------------------------------------------------
        write_silver(fs, df_ok, "s3://silver/vendas/", partition_cols=["ano", "mes"])
        logger.info("[OK] Tabela de Vendas processada e salva com sucesso na Silver!")

    except Exception as e:
        logger.error(f"[ERRO] Erro no processamento de Vendas: {str(e)}")
        raise


if __name__ == "__main__":
    main()
