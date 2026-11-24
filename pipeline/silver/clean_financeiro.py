import pandas as pd
import s3fs
import logging

from pipeline.settings import s3_storage_options
from pipeline.silver.text import clean_text


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("Iniciando processamento Silver para Financeiro")
    
    storage_options = s3_storage_options()
    
    try:
        fs = s3fs.S3FileSystem(**storage_options)
        arquivos = fs.glob("bronze/financeiro/**/*.csv")
        if not arquivos:
            logger.warning("Nenhum arquivo encontrado em bronze/financeiro/")
            return
            
        logger.info(f"Encontrados {len(arquivos)} arquivos na Bronze. Lendo dados...")
        df = pd.concat([pd.read_csv(f"s3://{arquivo}", storage_options=storage_options) for arquivo in arquivos], ignore_index=True)
        logger.info(f"Linhas lidas da Bronze: {len(df)}")
        
        df["_ingested_at"] = pd.to_datetime(df["_ingested_at"], errors="coerce")
        for col in ["valor_bruto", "valor_imposto", "valor_liquido"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        for col in ["tipo_lancamento", "status"]:
            df[col] = clean_text(df[col]).str.upper()
            
        df["competencia"] = clean_text(df["competencia"]).str.replace("/", "", regex=False)
        
        df["tipo_lancamento_norm"] = df["tipo_lancamento"].str.replace(".", "", regex=False)
        map_tipo = {
            "VENDA": "VENDA", "VENDAS": "VENDA", "VND": "VENDA", "V": "VENDA",
            "DEVOLUCAO": "DEVOLUCAO", "DEV": "DEVOLUCAO", "DEVOL": "DEVOLUCAO", "D": "DEVOLUCAO", "DEVOLUÇÃO": "DEVOLUCAO",
            "BONIFICACAO": "BONIFICACAO", "BONIF": "BONIFICACAO", "BONIFICAÇÃO": "BONIFICACAO",
            "AJUSTE": "AJUSTE", "OUTRO": "OUTRO"
        }
        df["tipo_lancamento_padrao"] = df["tipo_lancamento_norm"].map(map_tipo)
        
        df["competencia_dt"] = pd.to_datetime(df["competencia"], format="%Y%m", errors="coerce")
        map_status = {"PAGO": "PAGO", "PENDENTE": "PENDENTE", "CANCELADO": "CANCELADO"}
        df["status_padrao"] = df["status"].map(map_status)
        
        df = df.sort_values("_ingested_at").drop_duplicates(subset=["id_financeiro"], keep="last")
        
        df['competencia'] = df["competencia_dt"]
        df['ano'] = df['competencia'].dt.year
        df['mes'] = df['competencia'].dt.month
        
        df_silver = df[[
            "id_financeiro", "id_venda", "tipo_lancamento_padrao", 
            "competencia", "status_padrao", "ano", "mes"
        ]].copy()
        df_silver = df_silver.dropna(subset=["ano", "mes"])
        
        logger.info(f"Deduplicação concluída. Linhas finais: {len(df_silver)}")
        
        target_path_full = "s3://silver/financeiro/"
        
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
        logger.info("[OK] Tabela de Financeiro processada e salva com sucesso na Silver!")
        
    except Exception as e:
        logger.error(f"[ERRO] Erro no processamento de Financeiro: {str(e)}")
        raise

if __name__ == "__main__":
    main()