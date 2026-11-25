"""Acesso ao MinIO/S3: cliente, leitura da Bronze e escrita da Silver e da quarentena."""

import re
import time
import uuid

import pandas as pd
import s3fs

from pipeline.common.config import s3_storage_options
from pipeline.common.logging import get_logger

logger = get_logger(__name__)

QUARANTINE_PREFIX = "s3://silver/_quarentena"
# Áreas de apoio à escrita atômica, dentro do bucket do dataset (ver _write_atomically)
TMP_DIR = "_tmp"
PREV_DIR = "_prev"
# Erros transitórios do MinIO/S3 (ex.: "ClientDisconnected" no PutObject) acontecem de vez em quando.
# A gravação no prefixo temporário é privada e descartável, então refazê-la é seguro.
WRITE_ATTEMPTS = 3
WRITE_RETRY_DELAY_SECONDS = 2


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


PARTITION_DATE_PATTERN = re.compile(r"ingestion_date=(\d{4}-\d{2}-\d{2})$")


def _warn_legacy_partitions(fs: s3fs.S3FileSystem, table: str) -> None:
    """Partições antigas (`ingestion_date=<data>T<hora>`, de antes do ARQ-01) são ignoradas."""
    legadas = [
        p
        for p in fs.glob(f"bronze/{table}/ingestion_date=*")
        if not PARTITION_DATE_PATTERN.search(p.rstrip("/"))
    ]
    if legadas:
        logger.warning(
            f"{len(legadas)} partições Bronze antigas de '{table}' (com horário) são ignoradas; "
            f"veja a limpeza única no README. Exemplo: {legadas[0]}"
        )


def read_bronze(fs: s3fs.S3FileSystem, table: str, ds: str) -> pd.DataFrame | None:
    """Lê os CSVs da partição `bronze/<table>/ingestion_date=<ds>/`.

    Retorna None (com warning) se a partição não existir.
    """
    _warn_legacy_partitions(fs, table)
    arquivos = fs.glob(f"bronze/{table}/ingestion_date={ds}/*.csv")
    if not arquivos:
        logger.warning(f"Nenhum arquivo encontrado em bronze/{table}/ingestion_date={ds}/")
        return None

    logger.info(f"Lendo {len(arquivos)} arquivo(s) da partição ingestion_date={ds} de '{table}'...")
    storage_options = s3_storage_options()
    return pd.concat(
        [pd.read_csv(f"s3://{arquivo}", storage_options=storage_options) for arquivo in arquivos],
        ignore_index=True,
    )


def read_silver(path: str) -> pd.DataFrame:
    """Lê um dataset Parquet da Silver (arquivo ou diretório particionado)."""
    return pd.read_parquet(path, storage_options=s3_storage_options())


def read_silver_state(fs: s3fs.S3FileSystem, path: str) -> pd.DataFrame | None:
    """Estado atual de uma tabela Silver para o merge, ou None se ela ainda não existe.

    Colunas de partição voltam do Parquet como categóricas; aqui elas retornam ao tipo original,
    para que o merge e a regravação não misturem tipos.
    """
    if not fs.exists(_key(path)):
        return None
    df = read_silver(path)
    for coluna in df.select_dtypes("category").columns:
        df[coluna] = df[coluna].astype(df[coluna].cat.categories.dtype)
    return df


def _key(path: str) -> str:
    """Caminho do objeto sem o esquema e sem barra final: 's3://silver/vendas/' -> 'silver/vendas'."""
    return path.removeprefix("s3://").rstrip("/")


def _to_parquet(
    fs: s3fs.S3FileSystem, df: pd.DataFrame, key: str, partition_cols: list | None = None
) -> None:
    df.to_parquet(
        key,
        filesystem=fs,
        partition_cols=partition_cols,
        index=False,
        compression="snappy",
    )


def _validate_written(fs: s3fs.S3FileSystem, key: str, df: pd.DataFrame) -> None:
    """Relê o que foi gravado e confere linhas e colunas. Evita publicar uma escrita truncada."""
    lido = pd.read_parquet(key, filesystem=fs)
    if len(lido) != len(df) or set(lido.columns) != set(df.columns):
        raise RuntimeError(
            f"Validação da escrita falhou em {key}: gravado {len(lido)} linhas e "
            f"colunas {sorted(lido.columns)}; esperado {len(df)} linhas e colunas {sorted(df.columns)}"
        )


def _write_tmp_with_retry(
    fs: s3fs.S3FileSystem, df: pd.DataFrame, tmp: str, partition_cols: list | None
) -> None:
    """Grava e valida no prefixo temporário, refazendo do zero em caso de erro de E/S transitório."""
    for tentativa in range(1, WRITE_ATTEMPTS + 1):
        try:
            _to_parquet(fs, df, tmp, partition_cols)
            _validate_written(fs, tmp, df)
            return
        except OSError as e:
            if fs.exists(tmp):
                fs.rm(tmp, recursive=True)
            if tentativa == WRITE_ATTEMPTS:
                raise
            logger.warning(
                f"Erro de E/S gravando {tmp} (tentativa {tentativa}/{WRITE_ATTEMPTS}): {e}. Repetindo."
            )
            time.sleep(WRITE_RETRY_DELAY_SECONDS)


def _recover_interrupted_swap(fs: s3fs.S3FileSystem, key: str, prev: str) -> None:
    """Resolve o que uma troca interrompida (queda no meio da execução anterior) deixou em `_prev/`.

    - `_prev/` existe e o destino não: a execução caiu entre mover a versão antiga e publicar a
      nova. A versão antiga é restaurada.
    - os dois existem: a troca terminou e só faltou apagar `_prev/`.
    """
    if not fs.exists(prev):
        return
    if fs.exists(key):
        logger.warning(f"Troca anterior concluída, mas {prev} não foi apagado. Removendo.")
        fs.rm(prev, recursive=True)
    else:
        logger.warning(f"Troca anterior interrompida: restaurando {key} a partir de {prev}.")
        fs.mv(prev, key, recursive=True)


def _write_atomically(
    fs: s3fs.S3FileSystem,
    df: pd.DataFrame,
    path: str,
    partition_cols: list | None = None,
    run_id: str | None = None,
) -> None:
    """Substitui `path` por `df` sem nunca deixar o destino vazio ou parcial.

    O S3 não tem `rename` atômico, então o protocolo reduz o risco a uma troca curta:

      1. grava em `<bucket>/_tmp/<run_id>/<dataset>` e valida relendo;
      2. move a versão atual para `<bucket>/_prev/<dataset>`;
      3. move a nova para o destino (se falhar, a antiga é restaurada);
      4. apaga `_prev/`.

    Falha antes do passo 2 não toca na versão atual. Se o processo cair entre 2 e 3, a próxima
    escrita do mesmo dataset restaura `_prev/` antes de qualquer coisa.
    """
    run_id = run_id or uuid.uuid4().hex[:12]
    key = _key(path)
    bucket, _, dataset = key.partition("/")
    tmp = f"{bucket}/{TMP_DIR}/{run_id}/{dataset}"
    prev = f"{bucket}/{PREV_DIR}/{dataset}"

    _recover_interrupted_swap(fs, key, prev)
    # Sobras de execuções que caíram antes da troca (só deste dataset: tasks irmãs gravam em paralelo)
    for sobra in fs.glob(f"{bucket}/{TMP_DIR}/*/{dataset}"):
        if sobra != tmp:
            logger.warning(f"Removendo escrita temporária órfã: {sobra}")
            fs.rm(sobra, recursive=True)

    try:
        _write_tmp_with_retry(fs, df, tmp, partition_cols)

        if fs.exists(key):
            fs.mv(key, prev, recursive=True)
            try:
                fs.mv(tmp, key, recursive=True)
            except Exception:
                logger.error(f"Falha ao publicar {key}: restaurando a versão anterior.")
                if fs.exists(key):
                    fs.rm(key, recursive=True)
                fs.mv(prev, key, recursive=True)
                raise
            fs.rm(prev, recursive=True)
        else:
            fs.mv(tmp, key, recursive=True)
    finally:
        if fs.exists(f"{bucket}/{TMP_DIR}/{run_id}"):
            fs.rm(f"{bucket}/{TMP_DIR}/{run_id}", recursive=True)


def write_silver(
    fs: s3fs.S3FileSystem,
    df: pd.DataFrame,
    path: str,
    partition_cols: list | None = None,
    run_id: str | None = None,
) -> None:
    """Substitui o conteúdo de `path` na Silver pelo DataFrame (Parquet/snappy), de forma atômica.

    Uma falha em qualquer ponto preserva a versão anterior. Ver `_write_atomically`.
    """
    _write_atomically(fs, df, path, partition_cols, run_id)


def quarantine_path(entity: str, ds: str) -> str:
    """Caminho da quarentena de uma entidade em uma data lógica: `silver/_quarentena/<entidade>/<ds>`."""
    return f"{QUARANTINE_PREFIX}/{entity}/{ds}"


def write_quarantine(
    fs: s3fs.S3FileSystem, df: pd.DataFrame, entity: str, ds: str, run_id: str | None = None
) -> str:
    """Grava os registros rejeitados da data lógica `ds` em `_quarentena/<entidade>/<ds>`.

    Reexecutar a mesma data substitui só a quarentena daquela data (mesmo quando a nova vem
    vazia: nesse caso a anterior é removida e nada é gravado). As demais datas ficam como
    histórico. Retorna o caminho da quarentena da data.
    """
    path = quarantine_path(entity, ds)
    _remove_legacy_quarantine(fs, entity)
    if df.empty:
        if fs.exists(_key(path)):
            fs.rm(_key(path), recursive=True)
    else:
        _write_atomically(fs, df, path, run_id=run_id)
    return path


def _remove_legacy_quarantine(fs: s3fs.S3FileSystem, entity: str) -> None:
    """Antes do ARQ-05 a quarentena era UM arquivo `_quarentena/<entidade>`, o que colide com a pasta
    por data. Esse arquivo antigo é removido: a quarentena é reconstruída a cada execução."""
    legado = _key(f"{QUARANTINE_PREFIX}/{entity}")
    if fs.exists(legado) and fs.isfile(legado):
        logger.warning(f"Removendo quarentena no formato antigo (arquivo único): {legado}")
        fs.rm(legado)
