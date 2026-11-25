import sys

import pandas as pd

from pipeline.common.audit import record_dq
from pipeline.common.logging import get_logger
from pipeline.common.logical_date import resolve_ds
from pipeline.common.merge import merge_state
from pipeline.common.storage import (
    get_s3_client,
    read_bronze,
    read_silver_state,
    write_quarantine,
    write_silver,
)
from pipeline.common.validators import PATTERN_ID_PRODUTO, clean_text, normalize_id

logger = get_logger(__name__)

SILVER_PATH = "s3://silver/produtos/produtos_clean.parquet"

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


def validate_produtos(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Rejeita produtos com `id_produto` fora do padrão (vazio, formato inválido).

    Espera `id_produto` já normalizado (`normalize_id`). Retorna (aprovados, quarentena), esta com a
    coluna `motivo_rejeicao`.
    """
    invalido = ~df["id_produto"].str.match(PATTERN_ID_PRODUTO)
    quarentena = df[invalido].copy()
    quarentena["motivo_rejeicao"] = quarentena["id_produto"].map(
        lambda valor: f"id_produto inválido ou vazio: '{valor}'"
    )
    return df[~invalido].copy(), quarentena.reset_index(drop=True)


def main(ds: str | None = None, run_id: str | None = None):
    ds = resolve_ds(ds)
    logger.info(f"Iniciando processamento Silver para Produtos (partição {ds})")

    try:
        fs = get_s3_client()
        df = read_bronze(fs, "produtos", ds)
        if df is None:
            raise FileNotFoundError(f"Partição Bronze de produtos não encontrada para {ds}")
        lidas = len(df)
        logger.info(f"Linhas lidas da Bronze: {lidas}")

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

        df, df_quarentena = validate_produtos(df)
        if not df_quarentena.empty:
            logger.warning(
                f"{len(df_quarentena)} produtos com id_produto inválido ou vazio rejeitados: "
                f"{sorted(df_quarentena['id_produto'].unique().tolist())}"
            )

        df = df.sort_values("_ingested_at").drop_duplicates(subset=["id_produto"], keep="last")
        logger.info(f"Deduplicação concluída. Linhas finais: {len(df)}")

        df = flag_valores_indisponiveis(df)
        for col, flag in VALORES_MONETARIOS.items():
            sem_valor = sorted(df.loc[~df[flag], "id_produto"].tolist())
            if sem_valor:
                logger.warning(f"{len(sem_valor)} produtos sem {col} válido ({flag}=false): {sem_valor}")

        aprovadas = len(df)
        df = merge_state(read_silver_state(fs, SILVER_PATH), df, key=["id_produto"])
        write_silver(fs, df, SILVER_PATH)

        quarentena_path = write_quarantine(fs, df_quarentena, "produtos", ds)
        if not df_quarentena.empty:
            logger.warning(f"[QUARENTENA] {len(df_quarentena)} produtos salvos em {quarentena_path}")
        record_dq("produtos", ds, run_id, lidas, aprovadas, df_quarentena)
        logger.info("[OK] Tabela de Produtos salva com sucesso em silver/produtos/!")

    except Exception as e:
        logger.error(f"[ERRO] Erro no processamento de Produtos: {str(e)}")
        raise


if __name__ == "__main__":
    main(*sys.argv[1:3])
