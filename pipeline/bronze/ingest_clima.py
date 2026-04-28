import requests
import pandas as pd
import logging
from datetime import datetime
import s3fs
import io

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_URL = "https://archive-api.open-meteo.com/v1/archive?latitude=-21.78&longitude=-43.35&start_date=2024-01-01&end_date=2024-12-31&daily=temperature_2m_max,precipitation_sum&timezone=America%2FSao_Paulo"
API_BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Configuração do S3
fs = s3fs.S3FileSystem(
    key="minio",
    secret="minio123",
    client_kwargs={"endpoint_url": "http://localhost:9000"}
)

def ensure_bucket_exists(bucket_name):
    """Verifica e cria bucket se necessário"""
    try:
        if not fs.exists(bucket_name):
            logger.info(f"Criando bucket '{bucket_name}'...")
            fs.mkdir(bucket_name)
            logger.info(f"Bucket '{bucket_name}' criado")
            return True
        logger.info(f"Bucket '{bucket_name}' já existe")
        return True
    except Exception as e:
        logger.error(f"Erro ao verificar bucket: {e}")
        return False

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
        if not ensure_bucket_exists("bronze"):
            raise Exception("Não foi possível criar/verificar o bucket 'bronze'")

        df["_ingested_at"] = datetime.now()
        df["_api_source"] = API_BASE_URL

        ingestion_dt = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        path = f"bronze/clima/ingestion_date={ingestion_dt}/clima.csv"

        with fs.open(path, 'w') as f:
            df.to_csv(f, index=False)

        if fs.exists(path):
            logger.info(f"✓ Salvo com sucesso no MinIO: {path}")
            file_size = fs.size(path)
            logger.info(f"  Tamanho: {file_size} bytes")
        else:
            logger.error(f"✗ Arquivo não encontrado após salvamento")

    except Exception as e:
        logger.error(f"Erro ao salvar Bronze: {e}")
        raise

def main():
    logger.info("Iniciando ingestão clima -> Bronze (MinIO)")

    try:
        fs.ls('/')
        logger.info("✓ Conexão com MinIO estabelecida")
    except Exception as e:
        logger.error(f"✗ Erro ao conectar ao MinIO: {e}")
        return

    df = fetch_weather_data()
    save_to_bronze(df)

    logger.info("Ingestão finalizada")

if __name__ == "__main__":
    main()