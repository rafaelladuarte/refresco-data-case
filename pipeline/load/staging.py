import os
import pandas as pd
from sqlalchemy import create_engine, text
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_to_staging():
    logger.info("Iniciando carga de dados da camada Silver para Staging (Postgres-Gold)")
    
    # Configurações do MinIO
    endpoint_url = os.environ.get("AIRFLOW__S3__ENDPOINT_URL", "http://minio:9000")
    storage_options = {
        "key": "minio",
        "secret": "minio123",
        "client_kwargs": {"endpoint_url": endpoint_url}
    }
    
    # Configurações do Postgres (Target Gold)
    pg_user = os.environ.get("POSTGRES_USER", "uberlandia")
    pg_pass = os.environ.get("POSTGRES_PASSWORD", "senha123")
    pg_host = os.environ.get("POSTGRES_HOST", "postgres-gold")
    pg_port = os.environ.get("POSTGRES_PORT", "5432")
    pg_db = os.environ.get("POSTGRES_DB", "gold")
    
    engine_url = f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    engine = create_engine(engine_url)
    
    schema_name = 'silver_stage'
    
    # Criar o schema se não existir
    try:
        with engine.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name};"))
            logger.info(f"Schema '{schema_name}' verificado/criado com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao criar o schema {schema_name}: {e}")
        raise
        
    tabelas = ['vendas', 'produtos', 'clientes', 'clima', 'financeiro']
    
    for tabela in tabelas:
        logger.info(f"Iniciando carga da tabela: {tabela}")
        s3_path = f"s3://silver/{tabela}/"
        
        try:
            # Lendo os dados parquets do MinIO
            df = pd.read_parquet(s3_path, storage_options=storage_options)
            logger.info(f"Lidos {len(df)} registros da tabela {tabela} do S3.")
            
            # Parâmetros de escrita para o to_sql
            kwargs = {
                "name": tabela,
                "schema": schema_name,
                "con": engine,
                "if_exists": "replace",
                "index": False
            }
            
            # Otimização específica para vendas
            if tabela == 'vendas':
                kwargs["chunksize"] = 10000
                kwargs["method"] = 'multi'
                
            # Salvando no Postgres
            df.to_sql(**kwargs)
            logger.info(f"Tabela {tabela} carregada com sucesso no Postgres (Schema: {schema_name}).")
            
        except Exception as e:
            logger.error(f"Erro ao carregar a tabela {tabela}: {e}")
            raise
            
    logger.info("Carga de todos os arquivos para staging finalizada com sucesso!")

if __name__ == "__main__":
    load_to_staging()
