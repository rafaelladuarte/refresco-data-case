import pandas as pd
import s3fs
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fix_encoding(text):
    try:
        return text.encode("latin1").decode("utf-8")
    except:
        return text

def main():
    logger.info("Iniciando processamento Silver para Produtos")
    
    endpoint_url = os.environ.get("AIRFLOW__S3__ENDPOINT_URL", "http://minio:9000")
    storage_options = {
        "key": "minio",
        "secret": "minio123",
        "client_kwargs": {"endpoint_url": endpoint_url}
    }
    
    try:
        fs = s3fs.S3FileSystem(**storage_options)
        arquivos = fs.glob("bronze/produtos/**/*.csv")
        if not arquivos:
            logger.warning("Nenhum arquivo encontrado em bronze/produtos/")
            return
            
        df = pd.concat([pd.read_csv(f"s3://{arquivo}", storage_options=storage_options) for arquivo in arquivos], ignore_index=True)
        logger.info(f"Linhas lidas da Bronze: {len(df)}")
        
        # Encoding
        for col in ["nome_produto", "familia", "marca", "embalagem"]:
            df[col] = df[col].astype(str).apply(fix_encoding).str.strip()
        logger.info("Encoding corrigido.")
        
        # Unificação de categorias (familia)
        df["familia"] = df["familia"].str.upper()
        # Singular/Plural
        df["familia"] = df["familia"].replace({
            "REFRIGERANTES": "REFRIGERANTE", 
            "SUCOS": "SUCO", 
            "CERVEJAS": "CERVEJA", 
            "ÁGUAS": "AGUA", 
            "AGUAS": "AGUA"
        })
        
        # Limpeza de volume_ml (extrair numero)
        df["volume_ml"] = df["volume_ml"].astype(str).str.extract(r'(\d+)').astype(float)
        logger.info("Limpeza de volume concluída.")
        
        for col in ["preco_tabela", "custo_unitario"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            
        df["_ingested_at"] = pd.to_datetime(df["_ingested_at"], errors="coerce")
        df = df.sort_values("_ingested_at").drop_duplicates(subset=["id_produto"], keep="last")
        logger.info(f"Deduplicação concluída. Linhas finais: {len(df)}")
        
        target_path = "silver/produtos/"
        if fs.exists(target_path):
            fs.rm(target_path, recursive=True)
            logger.info(f"Diretório {target_path} limpo no S3 antes da reescrita.")
        
        df.to_parquet(
            path=f"s3://{target_path}produtos_clean.parquet",
            storage_options=storage_options,
            index=False,
            compression="snappy"
        )
        logger.info("✓ Dados salvos com sucesso na camada Silver (MinIO)!")
        
    except Exception as e:
        logger.error(f"✗ Erro no processamento de Produtos: {str(e)}")
        raise

if __name__ == "__main__":
    main()
