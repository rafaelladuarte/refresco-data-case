import requests
import pandas as pd
import logging
from datetime import datetime
import s3fs

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_URL = "https://archive-api.open-meteo.com/v1/archive?latitude=-21.78&longitude=-43.35&start_date=2024-01-01&end_date=2024-12-31&daily=temperature_2m_max,precipitation_sum&timezone=America%2FSao_Paulo"
API_BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

fs = s3fs.S3FileSystem(
    key="minio",
    secret="minio123",
    client_kwargs={"endpoint_url": "http://localhost:9000"}
)


def fetch_weather_data():
    try:
        logger.info("Consumindo API Open-Meteo...")
        response = requests.get(API_URL)

        if response.status_code != 200:
            logger.error(f"Erro API: {response.status_code}")
            return None

        data = response.json()
        daily = data.get("daily")

        if not daily:
            logger.warning("Sem dados retornados")
            return None

        df = pd.DataFrame(daily)
        logger.info(f"Registros: {len(df)}")

        return df

    except Exception as e:
        logger.error(str(e))
        return None


def save_to_bronze(df):
    if df is None or df.empty:
        logger.warning("DF vazio")
        return

    try:
        df["_ingested_at"] = datetime.now()
        df["_api_source"] = API_BASE_URL

        ingestion_dt = datetime.now().strftime("%Y-%m-%d")

        path = f"bronze/clima/ingestion_date={ingestion_dt}/clima.csv"

        df.to_csv(
            path,
            index=False
        )

        logger.info(f"Salvo no MinIO: {path}")

    except Exception as e:
        logger.error(f"Erro ao salvar Bronze: {e}")
        raise


def main():
    logger.info("Iniciando ingestão clima -> Bronze (MinIO)")

    df = fetch_weather_data()
    save_to_bronze(df)

    logger.info("Ingestão finalizada")


if __name__ == "__main__":
    main()