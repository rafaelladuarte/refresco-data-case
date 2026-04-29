import pandas as pd
import s3fs
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def normalize_ativo(value):
    if pd.isna(value):
        return None
    v = str(value).strip().lower()
    true_values = {"s", "sim", "1", "true", "t", "yes"}
    false_values = {"n", "nao", "não", "0", "false", "f", "no"}
    if v in true_values: return True
    elif v in false_values: return False
    return None

def main():
    logger.info("Iniciando processamento Silver para Clientes")
    
    endpoint_url = os.environ.get("AIRFLOW__S3__ENDPOINT_URL", "http://minio:9000")
    storage_options = {
        "key": "minio",
        "secret": "minio123",
        "client_kwargs": {"endpoint_url": endpoint_url}
    }
    
    try:
        fs = s3fs.S3FileSystem(**storage_options)
        arquivos = fs.glob("bronze/clientes/**/*.csv")
        
        if not arquivos:
            logger.warning("Nenhum arquivo encontrado em bronze/clientes/")
            return
            
        logger.info(f"Encontrados {len(arquivos)} arquivos. Lendo dados...")
        df = pd.concat([pd.read_csv(f"s3://{arquivo}", storage_options=storage_options) for arquivo in arquivos], ignore_index=True)
        logger.info(f"Linhas lidas da Bronze: {len(df)}")
        
        # Limpeza
        cols_str = ["nome_cliente", "canal", "regiao", "cidade", "uf", "representante"]
        for col in cols_str:
            df[col] = df[col].astype(str).str.strip()
            
        df["_ingested_at"] = pd.to_datetime(df["_ingested_at"], errors="coerce")
        df["uf"] = df["uf"].str.upper()
        df["ativo"] = df["ativo"].apply(normalize_ativo)
        
        # Filtros
        df = df[df["cnpj"].notna()]
        df = df[df["nome_cliente"].notna()]
        logger.info("Nulos críticos removidos.")
        
        # Deduplicar
        df = df.sort_values("_ingested_at")
        df = df.drop_duplicates(subset=["cnpj"], keep="last")
        logger.info(f"Deduplicação concluída. Linhas finais: {len(df)}")
        
        target_path = "silver/clientes/"
        if fs.exists(target_path):
            fs.rm(target_path, recursive=True)
            logger.info(f"Diretório {target_path} limpo no S3 antes da reescrita.")
        
        # Salvar
        df.to_parquet(
            path=f"s3://{target_path}",
            partition_cols=["regiao"],
            storage_options=storage_options,
            index=False,
            compression="snappy"
        )
        logger.info("✓ Dados salvos com sucesso na camada Silver (MinIO)!")
        
    except Exception as e:
        logger.error(f"✗ Erro no processamento de Clientes: {str(e)}")
        raise

if __name__ == "__main__":
    main()
