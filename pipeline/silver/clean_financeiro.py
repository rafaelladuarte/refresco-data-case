import pandas as pd

from pipeline.common.logging import get_logger
from pipeline.common.storage import get_s3_client, read_bronze, write_silver
from pipeline.common.validators import clean_text

logger = get_logger(__name__)

def main():
    logger.info("Iniciando processamento Silver para Financeiro")
    
    try:
        fs = get_s3_client()
        df = read_bronze(fs, "financeiro")
        if df is None:
            return
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
        
        write_silver(fs, df_silver, "s3://silver/financeiro/", partition_cols=["ano", "mes"])
        logger.info("[OK] Tabela de Financeiro processada e salva com sucesso na Silver!")
        
    except Exception as e:
        logger.error(f"[ERRO] Erro no processamento de Financeiro: {str(e)}")
        raise

if __name__ == "__main__":
    main()