
import pandas as pd
import s3fs
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("Iniciando processamento Silver para Clima")
    
    endpoint_url = os.getenv("AIRFLOW__S3__ENDPOINT_URL", "http://localhost:9000")
    storage_options = {
        "key": os.getenv("AWS_ACCESS_KEY_ID", "minio"),
        "secret": os.getenv("AWS_SECRET_ACCESS_KEY", "minio123"),
        "client_kwargs": {"endpoint_url": endpoint_url}
    }
    
    try:
        fs = s3fs.S3FileSystem(**storage_options)
        
        arquivos = fs.glob("bronze/clima/**/*.csv")
        
        if not arquivos:
            logger.warning("Nenhum arquivo encontrado em bronze/clima/")
            return
            
        logger.info(f"Encontrados {len(arquivos)} arquivos na Bronze. Lendo dados...")
        df = pd.concat([pd.read_csv(f"s3://{arquivo}", storage_options=storage_options) for arquivo in arquivos], ignore_index=True)
        logger.info(f"Linhas lidas inicialmente: {len(df)}")
        
        df["time"] = pd.to_datetime(df["time"], errors="coerce")
        df["_ingested_at"] = pd.to_datetime(df["_ingested_at"], errors="coerce")
        df["temperature_2m_max"] = pd.to_numeric(df["temperature_2m_max"], errors="coerce")
        df["precipitation_sum"] = pd.to_numeric(df["precipitation_sum"], errors="coerce")

        df = df.sort_values("_ingested_at")
        df = df.drop_duplicates(subset=["time"], keep="last")
        df = df.sort_values("time")

        Q1 = df["temperature_2m_max"].quantile(0.25)
        Q3 = df["temperature_2m_max"].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR

        df["temp_outlier"] = ~df["temperature_2m_max"].between(lower, upper) | ~df["temperature_2m_max"].between(-10, 50)

        p99 = df["precipitation_sum"].quantile(0.99)
        df["precip_outlier"] = (df["precipitation_sum"] > p99) | (df["precipitation_sum"] < 0)
        
        df['ano'] = df['time'].dt.year
        df['mes'] = df['time'].dt.month
        logger.info(f"Processamento concluído. Linhas finais: {len(df)}")
        
        df_silver = df[["time", "temperature_2m_max", "precipitation_sum", "_ingested_at", "_api_source", "ano", "mes"]]
        
        target_path_full = "s3://silver/clima/"
        
        if fs.exists(target_path_full):
            logger.info(f"Limpando diretório existente: {target_path_full}")
            fs.rm(target_path_full, recursive=True)
        
        df_silver.to_parquet(
            path=target_path_full,
            partition_cols=["ano", "mes"],
            storage_options=storage_options,
            index=False,
            compression="snappy"
        )
        logger.info("[OK] Tabela de Clima processada e salva com sucesso na Silver!")
        
    except Exception as e:
        logger.error(f"[ERRO] Erro no processamento de Clima: {str(e)}")
        raise

if __name__ == "__main__":
    main()