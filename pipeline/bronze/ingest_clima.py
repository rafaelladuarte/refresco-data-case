import io
import logging
import os
from dataclasses import dataclass
from datetime import datetime

import pandas as pd
import requests
import s3fs

from pipeline.settings import s3_storage_options


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

REQUEST_TIMEOUT_SECONDS = 30
DAILY_VARIABLES = "temperature_2m_max,precipitation_sum"
TIMEZONE = "America/Sao_Paulo"


@dataclass(frozen=True)
class ClimaConfig:
    """Parâmetros da ingestão climática, lidos do ambiente (sem segredos)."""
    api_url: str
    latitude: float
    longitude: float
    ano_inicio: int
    ano_fim: int

    @classmethod
    def from_env(cls) -> "ClimaConfig":
        config = cls(
            api_url=os.getenv("OPEN_METEO_ARCHIVE_URL", "https://archive-api.open-meteo.com/v1/archive"),
            # Padrão: Uberlândia-MG
            latitude=float(os.getenv("CLIMA_LATITUDE", "-18.9186")),
            longitude=float(os.getenv("CLIMA_LONGITUDE", "-48.2772")),
            ano_inicio=int(os.getenv("CLIMA_ANO_INICIO", "2020")),
            ano_fim=int(os.getenv("CLIMA_ANO_FIM", "2025")),
        )
        if config.ano_inicio > config.ano_fim:
            raise ValueError(
                f"CLIMA_ANO_INICIO ({config.ano_inicio}) maior que CLIMA_ANO_FIM ({config.ano_fim})"
            )
        return config


def get_s3_client():
    """Retorna uma nova instância do cliente S3FileSystem"""
    return s3fs.S3FileSystem(**s3_storage_options())


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


def fetch_weather_data_by_year(config: ClimaConfig, year: int) -> pd.DataFrame:
    """Busca dados climáticos diários de um ano. Erros de rede/HTTP são propagados."""
    params = {
        "latitude": config.latitude,
        "longitude": config.longitude,
        "start_date": f"{year}-01-01",
        "end_date": f"{year}-12-31",
        "daily": DAILY_VARIABLES,
        "timezone": TIMEZONE,
    }

    logger.info(
        f"Consumindo API Open-Meteo para o ano {year} "
        f"(latitude={config.latitude}, longitude={config.longitude})..."
    )
    response = requests.get(config.api_url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()

    daily = response.json().get("daily")
    if not daily:
        raise ValueError(f"Resposta sem bloco 'daily' para {year}")

    df = pd.DataFrame(daily)
    df["year"] = year
    logger.info(f"Registros para {year}: {len(df)}")
    return df


def fetch_all_weather_data(config: ClimaConfig) -> pd.DataFrame:
    """Busca todos os anos configurados.

    Falha em um ano isolado é registrada como warning e não interrompe os demais;
    se nenhum ano for obtido, a ingestão falha.
    """
    anos = list(range(config.ano_inicio, config.ano_fim + 1))
    all_data = []
    anos_com_falha = []

    for year in anos:
        try:
            all_data.append(fetch_weather_data_by_year(config, year))
        except (requests.RequestException, ValueError) as e:
            anos_com_falha.append(year)
            logger.warning(f"Falha ao obter clima de {year}: {e}")

    logger.info(f"Anos obtidos: {len(all_data)} de {len(anos)}")
    if anos_com_falha:
        logger.warning(f"Anos sem dados nesta execução: {anos_com_falha}")

    if not all_data:
        raise RuntimeError(
            f"Nenhum dado de clima retornado pela API para {config.ano_inicio}-{config.ano_fim}"
        )

    final_df = pd.concat(all_data, ignore_index=True)
    logger.info(f"Total de registros combinados ({config.ano_inicio}-{config.ano_fim}): {len(final_df)}")
    return final_df


def save_to_bronze(df: pd.DataFrame, config: ClimaConfig):
    fs = get_s3_client()
    try:
        if not ensure_bucket_exists(fs, "bronze"):
            raise Exception("Não foi possível criar/verificar o bucket 'bronze'")

        df["_ingested_at"] = datetime.now()
        df["_api_source"] = config.api_url
        df["latitude"] = config.latitude
        df["longitude"] = config.longitude

        ingestion_dt = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        path = (
            f"bronze/clima/ingestion_date={ingestion_dt}/"
            f"clima_{config.ano_inicio}_{config.ano_fim}.csv"
        )

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        with fs.open(path, 'wb') as f:
            f.write(csv_buffer.getvalue().encode("utf-8"))

        if not fs.exists(path):
            raise Exception(f"Arquivo não encontrado após salvamento: {path}")
        logger.info(f"[OK] Salvo com sucesso no MinIO: {path}")
        logger.info(f"Tamanho: {fs.size(path)} bytes")

    except Exception as e:
        logger.error(f"Erro ao salvar Bronze: {e}")
        raise


def main():
    config = ClimaConfig.from_env()
    logger.info(
        f"Iniciando ingestão clima -> Bronze (MinIO) para {config.ano_inicio}-{config.ano_fim} "
        f"(latitude={config.latitude}, longitude={config.longitude})"
    )

    fs = get_s3_client()
    try:
        fs.ls('/')
        logger.info("[OK] Conexão com MinIO estabelecida")
    except Exception as e:
        logger.error(f"[ERRO] Erro ao conectar ao MinIO: {e}")
        raise Exception(f"Falha de conexão com MinIO: {e}")

    df = fetch_all_weather_data(config)
    save_to_bronze(df, config)

    logger.info("[OK] Ingestão finalizada")


if __name__ == "__main__":
    main()
