import pandas as pd
import s3fs
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("Iniciando processamento Silver para Clima")
    
    endpoint_url = os.environ.get("AIRFLOW__S3__ENDPOINT_URL", "http://minio:9000")
    storage_options = {
        "key": "minio",
        "secret": "minio123",
        "client_kwargs": {"endpoint_url": endpoint_url}
    }
    
    try:
        fs = s3fs.S3FileSystem(**storage_options)
        arquivos = fs.glob("bronze/clima/**/*.csv")
        
        if not arquivos:
            logger.warning("Nenhum arquivo encontrado em bronze/clima/")
            return
            
        df = pd.concat([pd.read_csv(f"s3://{arquivo}", storage_options=storage_options) for arquivo in arquivos], ignore_index=True)
        logger.info(f"Linhas lidas da Bronze: {len(df)}")
        
        # Conversões
        df["time"] = pd.to_datetime(df["time"], errors="coerce")
        df["_ingested_at"] = pd.to_datetime(df["_ingested_at"], errors="coerce")
        df["temperature_2m_max"] = pd.to_numeric(df["temperature_2m_max"], errors="coerce")
        df["precipitation_sum"] = pd.to_numeric(df["precipitation_sum"], errors="coerce")
        
        # Deduplicar
        df = df.sort_values("_ingested_at")
        df = df.drop_duplicates(subset=["time"], keep="last")
        df = df.sort_values("time")
        logger.info(f"Deduplicação concluída. Linhas: {len(df)}")
        
        # Regras de Negócio / Outliers
        Q1 = df["temperature_2m_max"].quantile(0.25)
        Q3 = df["temperature_2m_max"].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR

        df["temp_outlier_iqr"] = ~df["temperature_2m_max"].between(lower, upper)
        df["temp_outlier_fisico"] = ~df["temperature_2m_max"].between(-10, 50)
        df["temp_outlier"] = df["temp_outlier_iqr"] | df["temp_outlier_fisico"]

        p99 = df["precipitation_sum"].quantile(0.99)
        df["precip_extremo"] = df["precipitation_sum"] > p99
        df["precip_invalido"] = df["precipitation_sum"] < 0
        df["precip_outlier"] = df["precip_extremo"] | df["precip_invalido"]
        
        df['ano'] = df['time'].dt.year
        df['mes'] = df['time'].dt.month
        logger.info("Verificação de outliers concluída.")
        
        df_silver = df[["time", "temperature_2m_max", "precipitation_sum", "_ingested_at", "_api_source", "ano", "mes"]]
        
        target_path = "silver/clima/"
        if fs.exists(target_path):
            fs.rm(target_path, recursive=True)
            logger.info(f"Diretório {target_path} limpo no S3 antes da reescrita.")
        
        df_silver.to_parquet(
            path=f"s3://{target_path}",
            partition_cols=["ano", "mes"],
            storage_options=storage_options,
            index=False,
            compression="snappy"
        )
        logger.info("✓ Dados salvos com sucesso na camada Silver (MinIO)!")
        
    except Exception as e:
        logger.error(f"✗ Erro no processamento de Clima: {str(e)}")
        raise

if __name__ == "__main__":
    main()
