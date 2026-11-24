"""Configuração tipada lida de variáveis de ambiente.

Credenciais nunca têm valor padrão: se a variável não existir, a execução falha
com uma mensagem explícita em vez de seguir com uma senha embutida no código.
"""
import os
from dataclasses import dataclass

DEFAULT_S3_ENDPOINT = "http://localhost:9000"
DEFAULT_OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"


def require_env(name: str) -> str:
    """Retorna o valor da variável de ambiente ou falha se ela não estiver definida."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Variável de ambiente obrigatória não definida: {name}")
    return value


@dataclass(frozen=True)
class S3Config:
    """Conexão com o MinIO/S3."""
    access_key: str
    secret_key: str
    endpoint_url: str

    @classmethod
    def from_env(cls) -> "S3Config":
        return cls(
            access_key=require_env("AWS_ACCESS_KEY_ID"),
            secret_key=require_env("AWS_SECRET_ACCESS_KEY"),
            endpoint_url=os.getenv("AIRFLOW__S3__ENDPOINT_URL", DEFAULT_S3_ENDPOINT),
        )

    def storage_options(self) -> dict:
        """Opções no formato aceito por s3fs e pelo `storage_options` do pandas."""
        return {
            "key": self.access_key,
            "secret": self.secret_key,
            "client_kwargs": {"endpoint_url": self.endpoint_url},
        }


@dataclass(frozen=True)
class PostgresConfig:
    """Conexão com o PostgreSQL da camada Gold."""
    user: str
    password: str
    db: str
    host: str
    port: str

    @classmethod
    def from_env(cls) -> "PostgresConfig":
        return cls(
            user=require_env("POSTGRES_USER"),
            password=require_env("POSTGRES_PASSWORD"),
            db=require_env("POSTGRES_DB"),
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5432"),
        )

    def url(self) -> str:
        """URL SQLAlchemy."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"


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
            api_url=os.getenv("OPEN_METEO_ARCHIVE_URL", DEFAULT_OPEN_METEO_URL),
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


def s3_storage_options() -> dict:
    """Atalho: opções de conexão S3 a partir do ambiente."""
    return S3Config.from_env().storage_options()


def postgres_url() -> str:
    """Atalho: URL SQLAlchemy do PostgreSQL da Gold a partir do ambiente."""
    return PostgresConfig.from_env().url()
