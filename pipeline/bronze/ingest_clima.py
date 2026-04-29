import requests
import pandas as pd
import logging
from datetime import datetime
import s3fs
import io
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_URL = "https://archive-api.open-meteo.com/v1/archive?latitude=-21.78&longitude=-43.35&start_date=2024-01-01&end_date=2024-12-31&daily=temperature_2m_max,precipitation_sum&timezone=America%2FSao_Paulo"
API_BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

def get_s3_client():
    """Retorna uma nova instância do cliente S3FileSystem"""
    endpoint_url = os.getenv("AIRFLOW__S3__ENDPOINT_URL", "http://localhost:9000")
    return s3fs.S3FileSystem(
        key=os.getenv("AWS_ACCESS_KEY_ID", "minio"),
        secret=os.getenv("AWS_SECRET_ACCESS_KEY", "minio123"),
        client_kwargs={"endpoint_url": endpoint_url}
    )

def ensure_bucket_exists(fs, bucket_name):
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

    fs = get_s3_client()
    try:
        if not ensure_bucket_exists(fs, "bronze"):
            raise Exception("Não foi possível criar/verificar o bucket 'bronze'")

        df["_ingested_at"] = datetime.now()
        df["_api_source"] = API_BASE_URL

        ingestion_dt = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        path = f"bronze/clima/ingestion_date={ingestion_dt}/clima.csv"

        with fs.open(path, 'w') as f:
            df.to_csv(f, index=False)

        if fs.exists(path):
            logger.info(f"[OK] Salvo com sucesso no MinIO: {path}")
            file_size = fs.size(path)
            logger.info(f"Tamanho: {file_size} bytes")
        else:
            logger.error(f"[ERRO] Arquivo não encontrado após salvamento")

    except Exception as e:
        logger.error(f"Erro ao salvar Bronze: {e}")
        raise

def main():
    logger.info("Iniciando ingestão clima -> Bronze (MinIO)")

    fs = get_s3_client()
    try:
        fs.ls('/')
        logger.info("[OK] Conexão com MinIO estabelecida")
    except Exception as e:
        logger.error(f"[ERRO] Erro ao conectar ao MinIO: {e}")
        raise Exception(f"Falha de conexão com MinIO: {e}")

    df = fetch_weather_data()
    if df is None or df.empty:
        raise ValueError("[ERRO] Falha: Nenhum dado de clima retornado pela API.")
        
    save_to_bronze(df)

    logger.info("[OK] Ingestão finalizada")

if __name__ == "__main__":
    main()