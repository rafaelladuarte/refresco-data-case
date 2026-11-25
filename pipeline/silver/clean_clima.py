import pandas as pd

from pipeline.common.logging import get_logger
from pipeline.common.storage import get_s3_client, read_bronze, write_silver

logger = get_logger(__name__)


def main():
    logger.info("Iniciando processamento Silver para Clima")

    try:
        fs = get_s3_client()
        df = read_bronze(fs, "clima")
        if df is None:
            return
        logger.info(f"Linhas lidas inicialmente: {len(df)}")

        df["time"] = pd.to_datetime(df["time"], errors="coerce")
        df["_ingested_at"] = pd.to_datetime(df["_ingested_at"], errors="coerce")
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
            ["time", "temperature_2m_max", "precipitation_sum", "_ingested_at", "_api_source", "ano", "mes"]
        ]

        write_silver(fs, df_silver, "s3://silver/clima/", partition_cols=["ano", "mes"])
        logger.info("[OK] Tabela de Clima processada e salva com sucesso na Silver!")

    except Exception as e:
        logger.error(f"[ERRO] Erro no processamento de Clima: {str(e)}")
        raise


if __name__ == "__main__":
    main()
