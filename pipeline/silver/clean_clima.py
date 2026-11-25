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

logger = get_logger(__name__)

SILVER_PATH = "s3://silver/clima/"


def validate_clima(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Rejeita medições sem data válida (`time` nula): sem data não há dia em `dim_tempo` nem partição.

    Retorna (aprovadas, quarentena), esta com a coluna `motivo_rejeicao`.
    """
    sem_data = df["time"].isna()
    quarentena = df[sem_data].assign(motivo_rejeicao="time inválido ou ausente")
    return df[~sem_data].copy(), quarentena.reset_index(drop=True)


def main(ds: str | None = None, run_id: str | None = None):
    ds = resolve_ds(ds)
    logger.info(f"Iniciando processamento Silver para Clima (partição {ds})")

    try:
        fs = get_s3_client()
        df = read_bronze(fs, "clima", ds)
        if df is None:
            raise FileNotFoundError(f"Partição Bronze de clima não encontrada para {ds}")
        lidas = len(df)
        logger.info(f"Linhas lidas inicialmente: {lidas}")

        df["time"] = pd.to_datetime(df["time"], errors="coerce")
        df["_ingested_at"] = pd.to_datetime(df["_ingested_at"], errors="coerce")
        df, df_quarentena = validate_clima(df)
        df["temperature_2m_max"] = pd.to_numeric(df["temperature_2m_max"], errors="coerce")
        df["precipitation_sum"] = pd.to_numeric(df["precipitation_sum"], errors="coerce")

        df = df.sort_values("_ingested_at")
        df = df.drop_duplicates(subset=["time"], keep="last")
        df = df.sort_values("time")

        Q1 = df["temperature_2m_max"].quantile(0.25)
        Q3 = df["temperature_2m_max"].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR

        df["temp_outlier"] = ~df["temperature_2m_max"].between(lower, upper) | ~df[
            "temperature_2m_max"
        ].between(-10, 50)

        p99 = df["precipitation_sum"].quantile(0.99)
        df["precip_outlier"] = (df["precipitation_sum"] > p99) | (df["precipitation_sum"] < 0)

        df["ano"] = df["time"].dt.year
        df["mes"] = df["time"].dt.month
        logger.info(f"Processamento concluído. Linhas finais: {len(df)}")

        df_silver = df[
            [
                "time",
                "temperature_2m_max",
                "precipitation_sum",
                "_ingested_at",
                "_logical_date",
                "_api_source",
                "ano",
                "mes",
            ]
        ]

        aprovadas = len(df_silver)
        df_silver = merge_state(read_silver_state(fs, SILVER_PATH), df_silver, key=["time"])
        write_silver(fs, df_silver, SILVER_PATH, partition_cols=["ano", "mes"])

        quarentena_path = write_quarantine(fs, df_quarentena, "clima", ds)
        if not df_quarentena.empty:
            logger.warning(f"[QUARENTENA] {len(df_quarentena)} medições salvas em {quarentena_path}")
        record_dq("clima", ds, run_id, lidas, aprovadas, df_quarentena)
        logger.info("[OK] Tabela de Clima processada e salva com sucesso na Silver!")

    except Exception as e:
        logger.error(f"[ERRO] Erro no processamento de Clima: {str(e)}")
        raise


if __name__ == "__main__":
    main(*sys.argv[1:3])
