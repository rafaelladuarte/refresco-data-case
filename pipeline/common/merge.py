"""Merge incremental da Silver: estado atual + partição nova da Bronze.

Cada tabela Silver guarda, além dos dados de negócio, quatro colunas de controle:

  _logical_date  data lógica (partição Bronze) de onde a linha veio
  _ingested_at   quando essa versão da linha foi ingerida
  _row_hash      hash do conteúdo de negócio (todas as colunas que não começam com `_`)
  _changed_at    quando o CONTEÚDO da linha mudou pela última vez (só avança se o hash mudar)

Regras do merge, por chave natural:

  1. precedência por (`_logical_date`, `_ingested_at`), nesta ordem: um backfill de uma data antiga
     nunca sobrescreve uma linha vinda de uma data mais nova, mesmo executado depois;
  2. reexecutar a mesma data é idempotente (mesmo resultado);
  3. `_changed_at` é preservado quando o hash da versão vencedora é igual ao do estado atual.
     Como cada dia reingere o snapshot inteiro, é isso que torna o incremental do dbt realmente
     incremental (D3 da Sprint 3);
  4. linha que some da origem permanece na Silver: o merge nunca apaga.
"""

import pandas as pd

CONTROL_COLUMNS = ["_logical_date", "_ingested_at", "_row_hash", "_changed_at"]
_VAZIO = "\x00"  # distingue nulo de texto vazio no hash


def row_hash(df: pd.DataFrame) -> pd.Series:
    """Hash do conteúdo de negócio de cada linha (colunas sem `_` no início), estável entre execuções.

    Tudo é convertido para string antes do hash: nulo (None, NaN, NaT) vira sempre o mesmo valor,
    e a ordem das colunas não importa.
    """
    cols = sorted(c for c in df.columns if not c.startswith("_"))
    texto = df[cols].astype("string").fillna(_VAZIO)
    return pd.util.hash_pandas_object(texto, index=False).astype("uint64").astype(str)


def _datas_em_ns(df: pd.DataFrame) -> pd.DataFrame:
    """Copia o DataFrame com as datas de controle em nanossegundos.

    O pandas 2.1 falha no `concat` ao misturar `datetime64[ns]` com outras resoluções, que podem vir
    do Parquet ou de um escalar `Timestamp`.
    """
    df = df.copy()
    for coluna in ("_ingested_at", "_changed_at"):
        if coluna in df:
            df[coluna] = pd.to_datetime(df[coluna]).astype("datetime64[ns]")
    return df


def _com_controle(atual: pd.DataFrame) -> pd.DataFrame:
    """Silver gravada antes do ARQ-01 não tem as colunas de controle: completa sem perder dados.

    Toda linha antiga recebe a data lógica mais antiga possível (qualquer partição nova a supera) e
    `_changed_at` = `_ingested_at`.
    """
    atual = atual.copy()
    if "_logical_date" not in atual:
        atual["_logical_date"] = "0000-00-00"
    if "_row_hash" not in atual:
        atual["_row_hash"] = row_hash(atual)
    if "_changed_at" not in atual:
        atual["_changed_at"] = atual["_ingested_at"]
    return atual


def merge_state(atual: pd.DataFrame | None, novo: pd.DataFrame, key: list[str]) -> pd.DataFrame:
    """Une o estado atual da Silver com as linhas novas, aplicando as regras do módulo.

    `novo` precisa ter `_logical_date` e `_ingested_at`. `atual` (se existir) precisa ter as quatro
    colunas de controle. O resultado vem ordenado pela chave.
    """
    novo = _datas_em_ns(novo)
    novo["_row_hash"] = row_hash(novo)
    novo["_changed_at"] = novo["_ingested_at"]

    if atual is None or atual.empty:
        return novo.sort_values(key, kind="mergesort").reset_index(drop=True)

    atual = _datas_em_ns(_com_controle(atual))
    atual["_origem"] = 0
    novo["_origem"] = 1  # empate total: a linha da execução corrente vence

    combinado = pd.concat([atual, novo], ignore_index=True)
    combinado = combinado.sort_values(["_logical_date", "_ingested_at", "_origem"], kind="mergesort")
    vencedores = combinado.drop_duplicates(subset=key, keep="last")

    anterior = atual[[*key, "_row_hash", "_changed_at"]].rename(
        columns={"_row_hash": "_hash_anterior", "_changed_at": "_changed_at_anterior"}
    )
    vencedores = vencedores.merge(anterior, on=key, how="left")
    inalterada = vencedores["_hash_anterior"].notna() & (
        vencedores["_hash_anterior"] == vencedores["_row_hash"]
    )
    vencedores["_changed_at"] = vencedores["_changed_at"].where(
        ~inalterada, vencedores["_changed_at_anterior"]
    )

    vencedores = vencedores.drop(columns=["_origem", "_hash_anterior", "_changed_at_anterior"])
    return vencedores.sort_values(key, kind="mergesort").reset_index(drop=True)
