"""Leitura de configuração e credenciais a partir de variáveis de ambiente.

Credenciais nunca têm valor padrão: se a variável não existir, a execução falha
com uma mensagem explícita em vez de seguir com uma senha embutida no código.
"""
import os

DEFAULT_S3_ENDPOINT = "http://localhost:9000"


def require_env(name: str) -> str:
    """Retorna o valor da variável de ambiente ou falha se ela não estiver definida."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Variável de ambiente obrigatória não definida: {name}")
    return value


def s3_storage_options() -> dict:
    """Opções de conexão com o MinIO/S3 no formato aceito por s3fs e pandas."""
    return {
        "key": require_env("AWS_ACCESS_KEY_ID"),
        "secret": require_env("AWS_SECRET_ACCESS_KEY"),
        "client_kwargs": {
            "endpoint_url": os.getenv("AIRFLOW__S3__ENDPOINT_URL", DEFAULT_S3_ENDPOINT)
        },
    }


def postgres_url() -> str:
    """URL SQLAlchemy do PostgreSQL da camada Gold."""
    user = require_env("POSTGRES_USER")
    password = require_env("POSTGRES_PASSWORD")
    db = require_env("POSTGRES_DB")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"
