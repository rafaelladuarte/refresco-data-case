import pandas as pd
import s3fs
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_mixed_dates(date_series):
    # Tenta dois formatos comuns e combina os resultados
    dt1 = pd.to_datetime(date_series, format="%Y-%m-%d", errors="coerce")
    dt2 = pd.to_datetime(date_series, format="%d/%m/%Y", errors="coerce")
    return dt1.combine_first(dt2)

def main():
    logger.info("Iniciando processamento Silver para Vendas")
    
    endpoint_url = os.environ.get("AIRFLOW__S3__ENDPOINT_URL", "http://minio:9000")
    storage_options = {
        "key": "minio",
        "secret": "minio123",
        "client_kwargs": {"endpoint_url": endpoint_url}
    }
    
    try:
        fs = s3fs.S3FileSystem(**storage_options)
        arquivos = fs.glob("bronze/vendas/**/*.csv")
        if not arquivos:
            logger.warning("Nenhum arquivo encontrado em bronze/vendas/")
            return
            
        df = pd.concat([pd.read_csv(f"s3://{arquivo}", storage_options=storage_options) for arquivo in arquivos], ignore_index=True)
        logger.info(f"Linhas lidas da Bronze: {len(df)}")
        
        # Tratamento de datas
        df["data_emissao"] = parse_mixed_dates(df["data_emissao"])
        
        df["ano"] = df["data_emissao"].dt.year
        df["mes"] = df["data_emissao"].dt.month
        logger.info("Conversão de datas e criação de ano/mês concluídas.")
        
        # Conversão numérica
        for col in ["qtd_caixas", "preco_unitario", "desconto_pct"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            
        # Cálculo receita líquida
        # Receita bruta = qtd_caixas * preco_unitario
        # Receita líquida = Receita bruta * (1 - desconto_pct)
        receita_bruta = df["qtd_caixas"] * df["preco_unitario"]
        df["receita_liquida"] = receita_bruta * (1 - df["desconto_pct"].fillna(0))
        logger.info("Cálculo de receita líquida concluído.")
        
        df["_ingested_at"] = pd.to_datetime(df["_ingested_at"], errors="coerce")
        df = df.sort_values("_ingested_at").drop_duplicates(subset=["id_venda"], keep="last")
        logger.info(f"Deduplicação concluída. Linhas finais: {len(df)}")
        
        df = df.dropna(subset=["data_emissao", "id_venda"])
        logger.info("Nulos críticos removidos.")
        
        target_path = "silver/vendas/"
        if fs.exists(target_path):
            fs.rm(target_path, recursive=True)
            logger.info(f"Diretório {target_path} limpo no S3 antes da reescrita.")
        
        df.to_parquet(
            path=f"s3://{target_path}",
            partition_cols=["ano", "mes"],
            storage_options=storage_options,
            index=False,
            compression="snappy"
        )
        logger.info("✓ Dados salvos com sucesso na camada Silver (MinIO)!")
        
    except Exception as e:
        logger.error(f"✗ Erro no processamento de Vendas: {str(e)}")
        raise

if __name__ == "__main__":
    main()
