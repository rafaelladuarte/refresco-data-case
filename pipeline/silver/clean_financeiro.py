import pandas as pd

from pipeline.common.logging import get_logger
from pipeline.common.storage import get_s3_client, read_bronze, write_silver
from pipeline.common.validators import clean_text

logger = get_logger(__name__)

# Variações de tipo_lancamento na origem (já em maiúsculas e sem pontos) → domínio padrão
MAP_TIPO_LANCAMENTO = {
    "VENDA": "VENDA", "VENDAS": "VENDA", "VND": "VENDA", "V": "VENDA",
    "DEVOLUCAO": "DEVOLUCAO", "DEV": "DEVOLUCAO", "DEVOL": "DEVOLUCAO", "D": "DEVOLUCAO", "DEVOLUÇÃO": "DEVOLUCAO",
    "BONIFICACAO": "BONIFICACAO", "BONIF": "BONIFICACAO", "BONIFICAÇÃO": "BONIFICACAO",
    "AJUSTE": "AJUSTE", "OUTRO": "OUTRO",
}
MAP_STATUS = {"PAGO": "PAGO", "PENDENTE": "PENDENTE", "CANCELADO": "CANCELADO"}


def normalize_tipo_lancamento(series: pd.Series) -> pd.Series:
    """Padroniza tipo_lancamento (ex.: 'bonif.' → 'BONIFICACAO'); valor desconhecido ou nulo vira nulo."""
    return clean_text(series).str.upper().str.replace(".", "", regex=False).map(MAP_TIPO_LANCAMENTO)


def normalize_status(series: pd.Series) -> pd.Series:
    """Padroniza status (ex.: ' pago ' → 'PAGO'); valor desconhecido ou nulo vira nulo."""
    return clean_text(series).str.upper().map(MAP_STATUS)


def parse_competencia(series: pd.Series) -> pd.Series:
    """Converte competência 'YYYYMM' ou 'YYYY/MM' em data (1º dia do mês); inválida vira NaT."""
    return pd.to_datetime(
        clean_text(series).str.replace("/", "", regex=False), format="%Y%m", errors="coerce"
    )


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
        df["tipo_lancamento_padrao"] = normalize_tipo_lancamento(df["tipo_lancamento"])
        df["status_padrao"] = normalize_status(df["status"])
        df["competencia"] = parse_competencia(df["competencia"])

        df = df.sort_values("_ingested_at").drop_duplicates(subset=["id_financeiro"], keep="last")

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