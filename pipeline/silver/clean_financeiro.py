import sys

import pandas as pd

from pipeline.common.audit import record_dq
from pipeline.common.logging import get_logger
from pipeline.common.logical_date import resolve_ds
from pipeline.common.merge import merge_state
from pipeline.common.storage import (
    get_s3_client,
    read_bronze,
    read_silver_state,
    write_quarantine,
    write_silver,
)
from pipeline.common.validators import clean_text

logger = get_logger(__name__)

SILVER_PATH = "s3://silver/financeiro/"

# Variações de tipo_lancamento na origem (já em maiúsculas e sem pontos) → domínio padrão
MAP_TIPO_LANCAMENTO = {
    "VENDA": "VENDA",
    "VENDAS": "VENDA",
    "VND": "VENDA",
    "V": "VENDA",
    "DEVOLUCAO": "DEVOLUCAO",
    "DEV": "DEVOLUCAO",
    "DEVOL": "DEVOLUCAO",
    "D": "DEVOLUCAO",
    "DEVOLUÇÃO": "DEVOLUCAO",
    "BONIFICACAO": "BONIFICACAO",
    "BONIF": "BONIFICACAO",
    "BONIFICAÇÃO": "BONIFICACAO",
    "AJUSTE": "AJUSTE",
    "OUTRO": "OUTRO",
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


def validate_financeiro(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Aplica as regras do financeiro: id presente, uma versão por lançamento e competência válida.

    Ordem: (1) `id_financeiro` vazio é rejeitado antes de deduplicar, para não juntar nulos num só;
    (2) vale a versão mais recente (`_ingested_at`) de cada `id_financeiro`; (3) o mais recente com
    competência inválida (`competencia` nula) é rejeitado. Espera `competencia_original` com o valor
    cru. Retorna (aprovados, quarentena), esta com a coluna `motivo_rejeicao`.
    """
    sem_id = df["id_financeiro"].isna() | (df["id_financeiro"].astype("string").str.strip() == "")
    q_id = df[sem_id].assign(motivo_rejeicao="id_financeiro vazio")

    df = df[~sem_id].sort_values("_ingested_at", kind="mergesort")
    df = df.drop_duplicates(subset=["id_financeiro"], keep="last")

    sem_competencia = df["competencia"].isna()
    q_competencia = df[sem_competencia].copy()
    q_competencia["motivo_rejeicao"] = q_competencia["competencia_original"].map(
        lambda valor: f"competencia inválida ou ausente: '{valor}'"
    )

    quarentena = pd.concat([q_id, q_competencia], ignore_index=True)
    return df[~sem_competencia].copy(), quarentena


def main(ds: str | None = None, run_id: str | None = None):
    ds = resolve_ds(ds)
    logger.info(f"Iniciando processamento Silver para Financeiro (partição {ds})")

    try:
        fs = get_s3_client()
        df = read_bronze(fs, "financeiro", ds)
        if df is None:
            raise FileNotFoundError(f"Partição Bronze de financeiro não encontrada para {ds}")
        lidas = len(df)
        logger.info(f"Linhas lidas da Bronze: {lidas}")

        df["_ingested_at"] = pd.to_datetime(df["_ingested_at"], errors="coerce")
        for col in ["valor_bruto", "valor_imposto", "valor_liquido"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["tipo_lancamento_padrao"] = normalize_tipo_lancamento(df["tipo_lancamento"])
        df["status_padrao"] = normalize_status(df["status"])
        df["competencia_original"] = df["competencia"]
        df["competencia"] = parse_competencia(df["competencia"])

        df, df_quarentena = validate_financeiro(df)

        df["ano"] = df["competencia"].dt.year
        df["mes"] = df["competencia"].dt.month

        df_silver = df[
            [
                "id_financeiro",
                "id_venda",
                "tipo_lancamento_padrao",
                "competencia",
                "status_padrao",
                "ano",
                "mes",
                "_ingested_at",
                "_logical_date",
            ]
        ].copy()
        aprovadas = len(df_silver)
        logger.info(f"Aprovados: {aprovadas}; rejeitados: {len(df_quarentena)}")

        df_silver = merge_state(read_silver_state(fs, SILVER_PATH), df_silver, key=["id_financeiro"])
        write_silver(fs, df_silver, SILVER_PATH, partition_cols=["ano", "mes"])

        quarentena_path = write_quarantine(fs, df_quarentena, "financeiro", ds)
        if not df_quarentena.empty:
            logger.warning(f"[QUARENTENA] {len(df_quarentena)} lançamentos salvos em {quarentena_path}")
        record_dq("financeiro", ds, run_id, lidas, aprovadas, df_quarentena)
        logger.info("[OK] Tabela de Financeiro processada e salva com sucesso na Silver!")

    except Exception as e:
        logger.error(f"[ERRO] Erro no processamento de Financeiro: {str(e)}")
        raise


if __name__ == "__main__":
    main(*sys.argv[1:3])
