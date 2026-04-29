import pandas as pd
import s3fs
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fix_encoding(text):
    """Corrige problemas de encoding (Latin1 para UTF-8)."""
    try:
        return text.encode("latin1").decode("utf-8")
    except:
        return text

def main():
    logger.info("Iniciando processamento Silver para Produtos")
    
    endpoint_url = os.getenv("AIRFLOW__S3__ENDPOINT_URL", "http://localhost:9000")
    storage_options = {
        "key": os.getenv("AWS_ACCESS_KEY_ID", "minio"),
        "secret": os.getenv("AWS_SECRET_ACCESS_KEY", "minio123"),
        "client_kwargs": {"endpoint_url": endpoint_url}
    }
    
    try:
        fs = s3fs.S3FileSystem(**storage_options)
        
        arquivos = fs.glob("bronze/produtos/**/*.csv")
        if not arquivos:
            logger.warning("Nenhum arquivo encontrado em bronze/produtos/")
            return
            
        logger.info(f"Encontrados {len(arquivos)} arquivos na Bronze. Lendo dados...")
        df = pd.concat([pd.read_csv(f"s3://{arquivo}", storage_options=storage_options) for arquivo in arquivos], ignore_index=True)
        logger.info(f"Linhas lidas da Bronze: {len(df)}")
        
        for col in ["nome_produto", "familia", "marca", "embalagem"]:
            df[col] = df[col].astype(str).apply(fix_encoding).str.strip()
        
        df["familia"] = df["familia"].str.upper().replace({
            "REFRIGERANTES": "REFRIGERANTE", 
            "SUCOS": "SUCO", 
            "CERVEJAS": "CERVEJA", 
            "ÁGUAS": "AGUA", 
            "AGUAS": "AGUA"
        })
        
        df["volume_ml"] = df["volume_ml"].astype(str).str.extract(r'(\d+)').astype(float)
        
        for col in ["preco_tabela", "custo_unitario"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            
        df["_ingested_at"] = pd.to_datetime(df["_ingested_at"], errors="coerce")
        
        df = df.sort_values("_ingested_at").drop_duplicates(subset=["id_produto"], keep="last")
        logger.info(f"Deduplicação concluída. Linhas finais: {len(df)}")
        
        target_path_full = "s3://silver/produtos/produtos_clean.parquet"
        
        if fs.exists(target_path_full):
            logger.info(f"Limpando diretório existente: {target_path_full}")
            fs.rm(target_path_full, recursive=True)
        
        df.to_parquet(
            path=target_path_full,
            storage_options=storage_options,
            index=False,
            compression="snappy"
        )
        logger.info("[OK] Tabela de Produtos salva com sucesso em silver/produtos/!")
        
    except Exception as e:
        logger.error(f"[ERRO] Erro no processamento de Produtos: {str(e)}")
        raise

if __name__ == "__main__":
    main()