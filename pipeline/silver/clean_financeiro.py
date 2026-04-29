import pandas as pd
import numpy as np
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
    logger.info("Iniciando processamento Silver para Financeiro")
    
    endpoint_url = os.environ.get("AIRFLOW__S3__ENDPOINT_URL", "http://minio:9000")
    storage_options = {
        "key": "minio",
        "secret": "minio123",
        "client_kwargs": {"endpoint_url": endpoint_url}
    }
    
    try:
        fs = s3fs.S3FileSystem(**storage_options)
        arquivos = fs.glob("bronze/financeiro/**/*.csv")
        if not arquivos:
            logger.warning("Nenhum arquivo encontrado em bronze/financeiro/")
            return
            
        df = pd.concat([pd.read_csv(f"s3://{arquivo}", storage_options=storage_options) for arquivo in arquivos], ignore_index=True)
        logger.info(f"Linhas lidas da Bronze: {len(df)}")
        
        df["_ingested_at"] = pd.to_datetime(df["_ingested_at"], errors="coerce")
        for col in ["valor_bruto", "valor_imposto", "valor_liquido"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            
        for col in ["tipo_lancamento", "status"]:
            df[col] = df[col].astype(str).str.strip().str.upper()
            
        df["competencia"] = df["competencia"].astype(str).str.replace("/", "", regex=False).str.strip()
        df["tipo_lancamento"] = df["tipo_lancamento"].apply(fix_encoding)
        logger.info("Encoding corrigido.")
        
        df["tipo_lancamento_norm"] = df["tipo_lancamento"].str.strip().str.upper().str.replace(".", "", regex=False)
        map_tipo = {
            "VENDA": "VENDA", "VENDAS": "VENDA", "VND": "VENDA", "V": "VENDA",
            "DEVOLUCAO": "DEVOLUCAO", "DEV": "DEVOLUCAO", "DEVOL": "DEVOLUCAO", "D": "DEVOLUCAO", "DEVOLUÇÃO": "DEVOLUCAO",
            "BONIFICACAO": "BONIFICACAO", "BONIF": "BONIFICACAO", "BONIFICAÇÃO": "BONIFICACAO",
            "AJUSTE": "AJUSTE", "OUTRO": "OUTRO"
        }
        df["tipo_lancamento_padrao"] = df["tipo_lancamento_norm"].map(map_tipo)
        
        df["competencia_dt"] = pd.to_datetime(df["competencia"], format="%Y%m", errors="coerce")
        df["status_norm"] = df["status"].astype(str).str.strip().str.upper().replace("NAN", np.nan)
        map_status = {"PAGO": "PAGO", "PENDENTE": "PENDENTE", "CANCELADO": "CANCELADO"}
        df["status_padrao"] = df["status_norm"].map(map_status)
        
        df["competencia"] = df["competencia_dt"]
        
        df = df.sort_values("_ingested_at").drop_duplicates(subset=["id_financeiro"], keep="last")
        logger.info(f"Deduplicação concluída. Linhas finais: {len(df)}")
        
        df['competencia'] = pd.to_datetime(df['competencia'], format='%Y-%m-%d', errors="coerce")
        df['ano'] = df['competencia'].dt.year
        df['mes'] = df['competencia'].dt.month
        
        df_silver = df[[
            "id_financeiro", "id_venda", "tipo_lancamento_padrao", 
            "competencia", "status_padrao", "ano", "mes"
        ]]
        
        target_path = "silver/financeiro/"
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
        logger.error(f"✗ Erro no processamento de Financeiro: {str(e)}")
        raise

if __name__ == "__main__":
    main()
