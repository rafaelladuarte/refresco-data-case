import io
import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

from pipeline.common.config import ClimaConfig
from pipeline.common.logging import get_logger
from pipeline.common.storage import ensure_bucket, get_s3_client

logger = get_logger(__name__)

REQUEST_TIMEOUT_SECONDS = 30
DAILY_VARIABLES = "temperature_2m_max,precipitation_sum"
TIMEZONE = "America/Sao_Paulo"


def _read_fixture(config: ClimaConfig, year: int) -> dict:
    """Lê a resposta gravada da API para o ano (modo CI, ver CLIMA_FIXTURE_DIR)."""
    path = Path(config.fixture_dir) / f"{year}.json"
    logger.info(f"Lendo fixture de clima {path} (CLIMA_FIXTURE_DIR definido, sem chamar a API)")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as e:
        raise ValueError(f"Fixture de clima não encontrada para {year}: {path}") from e


def fetch_weather_data_by_year(config: ClimaConfig, year: int) -> pd.DataFrame:
    """Busca dados climáticos diários de um ano. Erros de rede/HTTP são propagados."""
    if config.fixture_dir:
        payload = _read_fixture(config, year)
        return _payload_to_frame(payload, year)

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
    return _payload_to_frame(response.json(), year)


def _payload_to_frame(payload: dict, year: int) -> pd.DataFrame:
    daily = payload.get("daily")
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
        if not ensure_bucket(fs, "bronze"):
            raise Exception("Não foi possível criar/verificar o bucket 'bronze'")

        df["_ingested_at"] = datetime.now()
        df["_api_source"] = config.api_url
        df["latitude"] = config.latitude
        df["longitude"] = config.longitude

        ingestion_dt = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        path = (
            f"bronze/clima/ingestion_date={ingestion_dt}/" f"clima_{config.ano_inicio}_{config.ano_fim}.csv"
        )

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        with fs.open(path, "wb") as f:
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
        fs.ls("/")
        logger.info("[OK] Conexão com MinIO estabelecida")
    except Exception as e:
        logger.error(f"[ERRO] Erro ao conectar ao MinIO: {e}")
        raise Exception(f"Falha de conexão com MinIO: {e}") from e

    df = fetch_all_weather_data(config)
    save_to_bronze(df, config)

    logger.info("[OK] Ingestão finalizada")


if __name__ == "__main__":
    main()
