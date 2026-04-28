import pandas as pd
import os
from datetime import datetime
import logging
from pathlib import Path
import s3fs
import io

# Configuração de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configurações de Caminhos
LOCAL_RAW_DIR = Path("data/raw")  # Arquivos fonte locais
BRONZE_DIR = "bronze"  # Destino no MinIO

# Conexão com MinIO
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
    except Exception as e:
        logger.error(f"Erro ao verificar bucket: {e}")
        return False

def get_ingestion_date():
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

def get_timestamp():
    """Retorna o timestamp atual."""
    return datetime.now()

def process_table(table_name: str):
    """
    Lê um arquivo CSV da pasta local data/raw, 
    adiciona metadados e salva na camada Bronze no MinIO
    """
    source_file = LOCAL_RAW_DIR / f"{table_name}.csv"
    
    # Verifica se o arquivo existe localmente
    if not source_file.exists():
        logger.warning(f"Arquivo fonte não encontrado: {source_file}. Pulando...")
        return

    try:
        logger.info(f"Iniciando ingestão da tabela: {table_name}")
        
        # 1. Leitura do arquivo local
        df = pd.read_csv(source_file, encoding="latin1")
        logger.info(f"  - Registros lidos: {len(df)}")
        logger.info(f"  - Colunas: {list(df.columns)}")
        
        # 2. Enriquecimento com Metadados
        df['_ingested_at'] = get_timestamp()
        df['_source_file'] = f"{table_name}.csv"
        
        # 3. Definição do particionamento
        ingestion_date = get_ingestion_date()
        partition_path = f"{BRONZE_DIR}/{table_name}/ingestion_date={ingestion_date}"
        
        # 4. Garantir que o bucket bronze existe
        ensure_bucket_exists(BRONZE_DIR)
        
        # 5. Criar diretório de partição (se não existir)
        if not fs.exists(partition_path):
            fs.mkdir(partition_path)
            logger.info(f"  - Diretório criado: {partition_path}")
        
        target_path = f"{partition_path}/{table_name}.csv"
        
        # 6. Escrita na camada Bronze (CSV) no MinIO
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        
        with fs.open(target_path, 'w') as f:
            f.write(csv_buffer.getvalue())
        
        # Verificar se salvou
        if fs.exists(target_path):
            file_size = fs.size(target_path)
            logger.info(f"✓ Sucesso: {table_name} salvo no MinIO")
            logger.info(f"  - Caminho: {target_path}")
            logger.info(f"  - Tamanho: {file_size} bytes")
            logger.info(f"  - Partição: {ingestion_date}")
        else:
            logger.error(f"✗ Falha: arquivo não encontrado após salvamento")
        
        # 7. (Opcional) Mostrar preview dos dados
        logger.info(f"  - Preview dos dados:")
        logger.info(f"    {df.head(2).to_string()}")
        
    except Exception as e:
        logger.error(f"Erro ao processar a tabela {table_name}: {str(e)}")
        raise

def list_local_raw_files():
    """Lista os arquivos disponíveis em data/raw/"""
    if not LOCAL_RAW_DIR.exists():
        logger.error(f"Diretório {LOCAL_RAW_DIR} não encontrado!")
        logger.info(f"Crie o diretório e coloque os arquivos CSV lá")
        return []
    
    files = list(LOCAL_RAW_DIR.glob("*.csv"))
    if files:
        logger.info(f"\nArquivos encontrados em {LOCAL_RAW_DIR}:")
        for f in files:
            logger.info(f"  - {f.name}")
    else:
        logger.warning(f"Nenhum arquivo CSV encontrado em {LOCAL_RAW_DIR}")
    
    return files

def list_bronze_files():
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
                                logger.info(f"  - {file}")
                                logger.info(f"    Tamanho: {info['size']} bytes")
                                logger.info(f"    Modificado: {datetime.fromtimestamp(info['mtime'])}")
                else:
                    logger.info(f"  - Tabela '{table}' ainda não processada")
    except Exception as e:
        logger.error(f"Erro ao listar arquivos: {e}")

def main():
    """Função principal para coordenar a ingestão das entidades."""
    tables = ['clientes', 'financeiro', 'produtos', 'vendas']
    
    logger.info("=" * 60)
    logger.info("Pipeline de Ingestão: Data Local (data/raw/) -> MinIO (Bronze)")
    logger.info("=" * 60)
    
    # Verificar arquivos disponíveis localmente
    local_files = list_local_raw_files()
    
    if not local_files:
        logger.error("Nenhum arquivo encontrado. Encerrando pipeline.")
        return
    
    # Processar cada tabela
    for table in tables:
        logger.info("-" * 40)
        process_table(table)
    
    # Listar arquivos processados no MinIO
    list_bronze_files()
    
    logger.info("=" * 60)
    logger.info("✓ Pipeline de Ingestão finalizado com sucesso.")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()