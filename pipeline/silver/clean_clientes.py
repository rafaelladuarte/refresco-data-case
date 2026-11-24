import pandas as pd

from pipeline.common.logging import get_logger
from pipeline.common.storage import get_s3_client, read_bronze, write_quarantine, write_silver
from pipeline.common.validators import PATTERN_ID_CLIENTE, clean_text, normalize_id

logger = get_logger(__name__)

# Sequências típicas de texto UTF-8 decodificado como Latin-1 (ex.: "Ã©" no lugar de "é")
MOJIBAKE_MARKERS = ("Ã", "Â")


def repair_double_encoding(value):
    """Corrige texto com dupla codificação presente na própria origem.

    Alguns nomes em clientes.csv (arquivo Latin-1) foram gravados com bytes UTF-8,
    ex.: "Mercado do ZÃ©". Só tenta o reparo quando há marcadores de mojibake e
    mantém o valor original se a conversão não for válida, para não corromper
    textos legítimos.
    """
    if not isinstance(value, str) or not any(m in value for m in MOJIBAKE_MARKERS):
        return value
    try:
        return value.encode("latin1").decode("utf-8")
    except UnicodeError:
        return value


def normalize_ativo(value):
    """Normaliza valores booleanos da coluna 'ativo'."""
    if pd.isna(value):
        return None
    v = str(value).strip().lower()
    true_values = {"s", "sim", "1", "true", "t", "yes"}
    false_values = {"n", "nao", "não", "0", "false", "f", "no"}
    if v in true_values: return True
    elif v in false_values: return False
    return None

def deduplicate_clientes(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Aplica as regras de validação e deduplicação do cadastro de clientes.

    Regras (em ordem):
      1. id_cliente deve seguir PATTERN_ID_CLIENTE (vazio/nulo também é inválido).
      2. cnpj e nome_cliente são obrigatórios.
      3. Chave natural = id_cliente: mantém o registro mais recente por _ingested_at
         (ordenação estável, para que empates sejam determinísticos).
      4. CNPJ duplicado entre ids distintos: mantém o menor id_cliente (cadastro mais
         antigo) e envia os recadastros para a quarentena.

    Retorna:
      df_ok         — clientes válidos e únicos (→ Silver)
      df_quarentena — registros rejeitados, com coluna `motivo_rejeicao`
    """
    df = df.copy()
    df["id_cliente"] = normalize_id(df["id_cliente"])
    rejeitados = []

    def rejeitar(mask: pd.Series, motivo) -> None:
        nonlocal df
        if mask.any():
            bloco = df[mask].copy()
            bloco["motivo_rejeicao"] = bloco.apply(motivo, axis=1) if callable(motivo) else motivo
            rejeitados.append(bloco)
            df = df[~mask].copy()

    # Regra 1 — formato do id
    rejeitar(
        ~df["id_cliente"].str.match(PATTERN_ID_CLIENTE),
        lambda r: f"id_cliente inválido ou vazio: '{r['id_cliente']}'",
    )
    # Regra 2 — campos obrigatórios
    rejeitar(df["cnpj"].isna(), "cnpj vazio")
    rejeitar(df["nome_cliente"].isna(), "nome_cliente vazio")

    # Regra 3 — um registro por id_cliente (o mais recente)
    df = df.sort_values("_ingested_at", kind="mergesort")
    df = df.drop_duplicates(subset=["id_cliente"], keep="last")

    # Regra 4 — CNPJ duplicado: o menor id_cliente é o cadastro canônico
    canonico = df.groupby("cnpj")["id_cliente"].transform("min")
    df["_id_canonico"] = canonico
    rejeitar(
        df["id_cliente"] != df["_id_canonico"],
        lambda r: f"CNPJ duplicado de {r['_id_canonico']}",
    )
    df = df.drop(columns="_id_canonico")

    df_quarentena = (
        pd.concat(rejeitados, ignore_index=True).drop(columns="_id_canonico", errors="ignore")
        if rejeitados else df.iloc[0:0].assign(motivo_rejeicao=pd.Series(dtype="string"))
    )
    return df.sort_values("id_cliente").reset_index(drop=True), df_quarentena


def main():
    logger.info("Iniciando processamento Silver para Clientes")
    
    try:
        fs = get_s3_client()
        df = read_bronze(fs, "clientes")
        if df is None:
            return
        logger.info(f"Linhas lidas inicialmente: {len(df)}")
        
        cols_str = ["nome_cliente", "canal", "regiao", "cidade", "uf", "representante"]
        for col in cols_str:
            df[col] = clean_text(df[col])

        reparados = 0
        for col in cols_str:
            corrigido = df[col].apply(repair_double_encoding)
            reparados += int((corrigido != df[col]).sum())
            df[col] = corrigido
        if reparados:
            logger.warning(f"{reparados} valores com dupla codificação (mojibake) corrigidos na origem de clientes")
            
        df["_ingested_at"] = pd.to_datetime(df["_ingested_at"], errors="coerce")
        df["uf"] = df["uf"].str.upper()
        df["ativo"] = df["ativo"].apply(normalize_ativo)
        
        df, df_quarentena = deduplicate_clientes(df)
        logger.info(f"Clientes aprovados (→ Silver): {len(df)}")

        write_silver(fs, df, "s3://silver/clientes/", partition_cols=["regiao"])

        # Quarentena gravada após a Silver: o bucket 'silver' é criado na escrita acima
        for motivo, grupo in df_quarentena.groupby("motivo_rejeicao"):
            logger.warning(f"  [{len(grupo)} registros] {motivo}: {sorted(grupo['id_cliente'].tolist())}")
        quarentena_path = write_quarantine(fs, df_quarentena, "clientes")
        if not df_quarentena.empty:
            logger.warning(f"[QUARENTENA] {len(df_quarentena)} clientes salvos em {quarentena_path}")

        logger.info("[OK] Tabela de Clientes processada e salva com sucesso na Silver!")
        
    except Exception as e:
        logger.error(f"[ERRO] Erro no processamento de Clientes: {str(e)}")
        raise

if __name__ == "__main__":
    main()