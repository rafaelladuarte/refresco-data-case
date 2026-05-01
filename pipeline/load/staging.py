import os
import pandas as pd
from sqlalchemy import create_engine, text
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_to_staging():
    logger.info("Iniciando carga de dados da camada Silver para Staging (Postgres-Gold)")
    
    endpoint_url = os.getenv("AIRFLOW__S3__ENDPOINT_URL", "http://localhost:9000")
    storage_options = {
        "key": os.getenv("AWS_ACCESS_KEY_ID", "minio"),
        "secret": os.getenv("AWS_SECRET_ACCESS_KEY", "minio123"),
        "client_kwargs": {"endpoint_url": endpoint_url}
    }
    
    pg_user = os.getenv("POSTGRES_USER", "uberlandia")
    pg_pass = os.getenv("POSTGRES_PASSWORD", "senha123")
    pg_host = os.getenv("POSTGRES_HOST", "localhost")
    pg_port = os.getenv("POSTGRES_PORT", "5432")
    pg_db = os.getenv("POSTGRES_DB", "gold")
    
    engine_url = f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    engine = create_engine(engine_url)
    
    schema_name = 'load_stage'
    
    try:
        with engine.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name};"))
            logger.info(f"[OK] Schema '{schema_name}' verificado/criado com sucesso.")
    except Exception as e:
        logger.error(f"[ERRO] Erro ao criar o schema {schema_name}: {e}")
        raise
        
    tabelas = ['vendas', 'produtos', 'clientes', 'clima', 'financeiro']
    
    for tabela in tabelas:
        logger.info(f"Iniciando carga da tabela: {tabela}")
        s3_path = f"s3://silver/{tabela}/"
        
        try:
            df = pd.read_parquet(s3_path, storage_options=storage_options)
            logger.info(f"Lidos {len(df)} registros da tabela {tabela} do S3.")
            
            kwargs = {
                "name": tabela,
                "schema": schema_name,
                "if_exists": "replace",
                "index": False
            }
            
            if tabela == 'vendas':
                kwargs["chunksize"] = 10000
                kwargs["method"] = 'multi'
                
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {schema_name}.{tabela} CASCADE;"))
                logger.info(f"DROP TABLE CASCADE executado com sucesso para {schema_name}.{tabela}.")
                
                kwargs["con"] = conn
                df.to_sql(**kwargs)
                
            logger.info(f"[OK] Tabela {tabela} carregada com sucesso no Postgres (Schema: {schema_name}).")
            
        except Exception as e:
            logger.error(f"[ERRO] Erro ao carregar a tabela {tabela}: {e}")
            raise
            
    logger.info("Carga de todos os arquivos para staging finalizada com sucesso!")

if __name__ == "__main__":
    load_to_staging()
