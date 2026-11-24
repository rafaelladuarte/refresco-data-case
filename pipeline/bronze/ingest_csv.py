import io
import os
from datetime import datetime
from pathlib import Path

import pandas as pd

from pipeline.common.logging import get_logger
from pipeline.common.storage import ensure_bucket, get_s3_client

logger = get_logger(__name__)

airflow_home = os.getenv("AIRFLOW_HOME", "/opt/airflow")
LOCAL_RAW_DIR = Path(f"{airflow_home}/data/raw")
BRONZE_DIR = "bronze"

# Encoding de cada arquivo de origem (contrato da fonte). Somente clientes.csv é
# ISO-8859-1; os demais são UTF-8. Ler tudo como latin1 corrompia acentos
# (ex.: "Conveniência" virava "ConveniÃªncia"). A Bronze é sempre gravada em UTF-8.
SOURCE_ENCODINGS = {
    "clientes": "latin1",
    "financeiro": "utf-8",
    "produtos": "utf-8",
    "vendas": "utf-8",
}
BRONZE_ENCODING = "utf-8"

def get_ingestion_date():
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

def get_timestamp():
    """Retorna o timestamp atual."""
    return datetime.now()

def process_table(fs, table_name: str):
    """
    Lê um arquivo CSV da pasta local data/raw, 
    adiciona metadados e salva na camada Bronze no MinIO
    """
    source_file = LOCAL_RAW_DIR / f"{table_name}.csv"

    if not source_file.exists():
        msg = f"Arquivo fonte não encontrado: {source_file}."
        logger.error(msg)
        raise FileNotFoundError(msg)

    try:
        logger.info(f"Iniciando ingestão da tabela: {table_name}")
        
        if table_name not in SOURCE_ENCODINGS:
            raise ValueError(f"Encoding não definido para a tabela '{table_name}' em SOURCE_ENCODINGS")
        encoding = SOURCE_ENCODINGS[table_name]
        df = pd.read_csv(source_file, encoding=encoding)
        logger.info(f"Encoding de origem: {encoding}")
        logger.info(f"Registros lidos: {len(df)}")
        logger.info(f"Colunas: {list(df.columns)}")
        
        df['_ingested_at'] = get_timestamp()
        df['_source_file'] = f"{table_name}.csv"
        
        ingestion_date = get_ingestion_date()
        partition_path = f"{BRONZE_DIR}/{table_name}/ingestion_date={ingestion_date}"
        
        if not ensure_bucket(fs, BRONZE_DIR):
            raise Exception("Não foi possível verificar/criar o bucket bronze")
        
        if not fs.exists(partition_path):
            fs.mkdir(partition_path)
            logger.info(f"Diretório criado: {partition_path}")
        
        target_path = f"{partition_path}/{table_name}.csv"
        
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        # Grava em modo binário com encoding explícito para não depender do locale do container
        with fs.open(target_path, 'wb') as f:
            f.write(csv_buffer.getvalue().encode(BRONZE_ENCODING))
        
        if fs.exists(target_path):
            file_size = fs.size(target_path)
            logger.info(f"[OK] Sucesso: {table_name} salvo no MinIO")
            logger.info(f"- Caminho: {target_path}")
            logger.info(f"- Tamanho: {file_size} bytes")
            logger.info(f"- Partição: {ingestion_date}")
        else:
            logger.error(f"[ERRO] Falha: arquivo não encontrado após salvamento")
            raise Exception(f"Falha ao salvar o arquivo {target_path}")
        
    except Exception as e:
        logger.error(f"Erro ao processar a tabela {table_name}: {str(e)}")
        raise

def list_local_raw_files():
    """Lista os arquivos disponíveis em data/raw/"""
    if not LOCAL_RAW_DIR.exists():
        msg = f"Diretório {LOCAL_RAW_DIR} não encontrado!"
        logger.error(msg)
        raise FileNotFoundError(msg)
    
    files = list(LOCAL_RAW_DIR.glob("*.csv"))
    if files:
        logger.info(f"\nArquivos encontrados em {LOCAL_RAW_DIR}:")
        for f in files:
            logger.info(f"  - {f.name}")
    else:
        msg = f"Nenhum arquivo CSV encontrado em {LOCAL_RAW_DIR}"
        logger.error(msg)
        raise FileNotFoundError(msg)
    
    return files

def list_bronze_files(fs):
    """Lista todos os arquivos processados na camada Bronze no MinIO"""
    try:
        if fs.exists(BRONZE_DIR):
            logger.info("\n=== Arquivos processados no Bronze (MinIO) ===")
            for table in ['clientes', 'financeiro', 'produtos', 'vendas']:
                table_path = f"{BRONZE_DIR}/{table}"
                if fs.exists(table_path):
                    partitions = fs.ls(table_path)
                    for partition in partitions:
                        files = fs.ls(partition)
                        for file in files:
                            if file.endswith('.csv'):
                                info = fs.stat(file)
                                logger.info(f"{file}")
                                logger.info(f"- Tamanho: {info['size']} bytes")
                                # Objetos S3 expõem 'LastModified' (datetime), não 'mtime'
                                logger.info(f"- Modificado: {info.get('LastModified')}")
                else:
                    logger.info(f"- Tabela '{table}' ainda não processada")
    except Exception as e:
        logger.error(f"Erro ao listar arquivos: {e}")

def main():
    """Função principal para coordenar a ingestão das entidades."""
    tables = ['clientes', 'financeiro', 'produtos', 'vendas']
    
    logger.info(f"Pipeline de Ingestão: Data Local ({LOCAL_RAW_DIR}) -> MinIO (Bronze)")
    
    local_files = list_local_raw_files()
    
    fs = get_s3_client()
    
    for table in tables:
        process_table(fs, table)
    
    list_bronze_files(fs)
    
    logger.info("[OK] Pipeline de Ingestão finalizado com sucesso.")

if __name__ == "__main__":
    main()