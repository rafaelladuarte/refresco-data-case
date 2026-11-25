"""Acesso ao MinIO/S3: cliente, leitura da Bronze e escrita da Silver e da quarentena."""

import pandas as pd
import s3fs

from pipeline.common.config import s3_storage_options
from pipeline.common.logging import get_logger

logger = get_logger(__name__)

QUARANTINE_PREFIX = "s3://silver/_quarentena"


def get_s3_client() -> s3fs.S3FileSystem:
    """Retorna um cliente S3FileSystem configurado pelo ambiente."""
    return s3fs.S3FileSystem(**s3_storage_options())


def ensure_bucket(fs: s3fs.S3FileSystem, bucket_name: str) -> bool:
    """Cria o bucket se ele não existir. Retorna False (e registra o erro) se não conseguir."""
    try:
        if not fs.exists(bucket_name):
            logger.info(f"Criando bucket '{bucket_name}'...")
            fs.mkdir(bucket_name)
            logger.info(f"Bucket '{bucket_name}' criado")
        return True
    except Exception as e:
        logger.error(f"Erro ao verificar bucket: {e}")
        return False


def read_bronze(fs: s3fs.S3FileSystem, table: str) -> pd.DataFrame | None:
    """Lê e concatena todos os CSVs de todas as partições de `bronze/<table>/`.

    Retorna None (com warning) se não houver arquivos.
    """
    arquivos = fs.glob(f"bronze/{table}/**/*.csv")
    if not arquivos:
        logger.warning(f"Nenhum arquivo encontrado em bronze/{table}/")
        return None

    logger.info(f"Encontrados {len(arquivos)} arquivos na Bronze. Lendo dados...")
    storage_options = s3_storage_options()
    return pd.concat(
        [pd.read_csv(f"s3://{arquivo}", storage_options=storage_options) for arquivo in arquivos],
        ignore_index=True,
    )


def read_silver(path: str) -> pd.DataFrame:
    """Lê um dataset Parquet da Silver (arquivo ou diretório particionado)."""
    return pd.read_parquet(path, storage_options=s3_storage_options())


def _to_parquet(df: pd.DataFrame, path: str, partition_cols: list | None = None) -> None:
    df.to_parquet(
        path=path,
        partition_cols=partition_cols,
        storage_options=s3_storage_options(),
        index=False,
        compression="snappy",
    )


def write_silver(
    fs: s3fs.S3FileSystem, df: pd.DataFrame, path: str, partition_cols: list | None = None
) -> None:
    """Substitui o conteúdo de `path` na Silver pelo DataFrame (rm + escrita em Parquet/snappy)."""
    if fs.exists(path):
        logger.info(f"Limpando diretório existente: {path}")
        fs.rm(path, recursive=True)
    _to_parquet(df, path, partition_cols)


def write_quarantine(fs: s3fs.S3FileSystem, df: pd.DataFrame, entity: str) -> str:
    """Substitui a quarentena da entidade pelos registros rejeitados desta execução.

    A quarentena da execução anterior é sempre removida, mesmo que a atual venha
    vazia (nesse caso nada é gravado). Retorna o caminho da quarentena.
    """
    path = f"{QUARANTINE_PREFIX}/{entity}/"
    if fs.exists(path):
        fs.rm(path, recursive=True)
    if not df.empty:
        _to_parquet(df, path)
    return path
