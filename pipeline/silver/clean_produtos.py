import pandas as pd

from pipeline.common.logging import get_logger
from pipeline.common.storage import get_s3_client, read_bronze, write_silver
from pipeline.common.validators import PATTERN_ID_PRODUTO, clean_text, normalize_id

logger = get_logger(__name__)

# Valor monetário sem significado (<= 0 ou ausente) vira nulo e ganha a flag `<flag> = False`
VALORES_MONETARIOS = {"custo_unitario": "custo_disponivel", "preco_tabela": "preco_disponivel"}


def flag_valores_indisponiveis(df: pd.DataFrame) -> pd.DataFrame:
    """Sinaliza custo e preço indisponíveis sem descartar o produto (DQ-09, opção A).

    `custo_unitario`/`preco_tabela` nulo, não numérico ou <= 0 vira NULL, e a flag
    correspondente (`custo_disponivel`/`preco_disponivel`) fica False. O produto continua
    na Silver: as vendas dele seguem contando em receita e volume, e só a margem deixa de
    ser calculada (custo zero produziria margem de 100%).
    """
    df = df.copy()
    for col, flag in VALORES_MONETARIOS.items():
        valor = pd.to_numeric(df[col], errors="coerce")
        disponivel = valor > 0
        df[col] = valor.where(disponivel)
        df[flag] = disponivel.astype(bool)
    return df


def main():
    logger.info("Iniciando processamento Silver para Produtos")

    try:
        fs = get_s3_client()
        df = read_bronze(fs, "produtos")
        if df is None:
            return
        logger.info(f"Linhas lidas da Bronze: {len(df)}")

        for col in ["nome_produto", "familia", "marca", "embalagem"]:
            df[col] = clean_text(df[col])

        df["familia"] = (
            df["familia"]
            .str.upper()
            .replace(
                {
                    "REFRIGERANTES": "REFRIGERANTE",
                    "SUCOS": "SUCO",
                    "CERVEJAS": "CERVEJA",
                    "ÁGUAS": "AGUA",
                    "AGUAS": "AGUA",
                }
            )
        )

        df["volume_ml"] = df["volume_ml"].astype(str).str.extract(r"(\d+)").astype(float)

        for col in ["preco_tabela", "custo_unitario"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df["_ingested_at"] = pd.to_datetime(df["_ingested_at"], errors="coerce")

        # Normaliza id_produto: remove espaços e descarta formatos inválidos
        df["id_produto"] = normalize_id(df["id_produto"])

        mask_id_invalido = ~df["id_produto"].str.match(PATTERN_ID_PRODUTO)
        df_id_invalido = df[mask_id_invalido].copy()
        if not df_id_invalido.empty:
            logger.warning(
                f"{len(df_id_invalido)} produtos com id_produto inválido ou vazio descartados: "
                f"{sorted(df_id_invalido['id_produto'].unique().tolist())}"
            )
        df = df[~mask_id_invalido].copy()

        df = df.sort_values("_ingested_at").drop_duplicates(subset=["id_produto"], keep="last")
        logger.info(f"Deduplicação concluída. Linhas finais: {len(df)}")

        df = flag_valores_indisponiveis(df)
        for col, flag in VALORES_MONETARIOS.items():
            sem_valor = sorted(df.loc[~df[flag], "id_produto"].tolist())
            if sem_valor:
                logger.warning(f"{len(sem_valor)} produtos sem {col} válido ({flag}=false): {sem_valor}")

        write_silver(fs, df, "s3://silver/produtos/produtos_clean.parquet")
        logger.info("[OK] Tabela de Produtos salva com sucesso em silver/produtos/!")

    except Exception as e:
        logger.error(f"[ERRO] Erro no processamento de Produtos: {str(e)}")
        raise


if __name__ == "__main__":
    main()
