"""Carga da Silver no schema `load_stage` do Postgres (ARQ-03).

As tabelas são definidas em `pipeline/ddl/load_stage.sql` (tipos explícitos) e nunca são recriadas
numa execução normal: cada carga faz `TRUNCATE` + `COPY` de todas elas numa única transação. Assim as
views `stg_*` do dbt sobrevivem à carga, e uma falha no meio (rollback) preserva a carga anterior.

Duas situações são tratadas à parte:
  * a Silver tem colunas diferentes das da DDL (coluna nova, removida): a carga FALHA, antes de abrir
    a transação, listando a diferença (desvio de schema pego cedo);
  * uma tabela já existe com colunas ou tipos diferentes da DDL (ambiente criado antes do ARQ-03, em
    que o pandas inferia os tipos): ela é recriada uma única vez, com aviso. Isso derruba as views
    `stg_*` dependentes, que o próximo `dbt run` recria.
"""

import io
import re
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

from pipeline.common.config import postgres_url
from pipeline.common.logging import get_logger
from pipeline.common.storage import read_silver

logger = get_logger(__name__)

SCHEMA = "load_stage"
DDL_PATH = Path(__file__).resolve().parents[1] / "ddl" / "load_stage.sql"

# tabela -> caminho na Silver
SILVER_PATHS = {
    "vendas": "s3://silver/vendas/",
    "produtos": "s3://silver/produtos/",
    "clientes": "s3://silver/clientes/",
    "clima": "s3://silver/clima/",
    "financeiro": "s3://silver/financeiro/",
}

# Marcador de nulo no CSV do COPY: diferencia NULL de texto vazio
NULL_MARK = "\\N"

# Tipo escrito na DDL -> `data_type` do information_schema
_TIPOS_PG = {
    "text": "text",
    "integer": "integer",
    "double precision": "double precision",
    "boolean": "boolean",
    "timestamp": "timestamp without time zone",
}

_CREATE_TABLE = re.compile(
    rf"CREATE TABLE IF NOT EXISTS {SCHEMA}\.(\w+)\s*\((.*?)\)\s*;", re.DOTALL | re.IGNORECASE
)


def parse_ddl(sql: str) -> dict[str, list[tuple[str, str]]]:
    """Extrai {tabela: [(coluna, tipo), ...]} da DDL, na ordem em que as colunas aparecem."""
    tabelas: dict[str, list[tuple[str, str]]] = {}
    for tabela, corpo in _CREATE_TABLE.findall(sql):
        colunas = []
        for linha in corpo.split(","):
            linha = linha.strip()
            if not linha:
                continue
            nome, _, tipo = linha.partition(" ")
            tipo = tipo.strip()
            if tipo not in _TIPOS_PG:
                raise ValueError(f"Tipo não suportado na DDL de {tabela}.{nome}: '{tipo}'")
            colunas.append((nome, tipo))
        tabelas[tabela] = colunas
    return tabelas


def validate_columns(tabela: str, colunas_silver: list[str], spec: list[tuple[str, str]]) -> None:
    """Falha, listando a diferença, se a Silver e a DDL não tiverem exatamente as mesmas colunas."""
    esperadas = {nome for nome, _ in spec}
    obtidas = set(colunas_silver)
    faltando, sobrando = sorted(esperadas - obtidas), sorted(obtidas - esperadas)
    if faltando or sobrando:
        raise ValueError(
            f"Desvio de schema em {SCHEMA}.{tabela}: a Silver não bate com pipeline/ddl/load_stage.sql. "
            f"Faltam na Silver: {faltando}; sobram na Silver: {sobrando}. "
            "Atualize a DDL (e os modelos dbt que dependem dela) antes de carregar."
        )


def prepare_frame(df: pd.DataFrame, spec: list[tuple[str, str]]) -> pd.DataFrame:
    """Ordena as colunas como na DDL e converte cada uma para o tipo da tabela."""
    df = df[[nome for nome, _ in spec]].copy()
    for nome, tipo in spec:
        coluna = df[nome]
        if isinstance(coluna.dtype, pd.CategoricalDtype):  # colunas de partição vêm como categóricas
            coluna = coluna.astype(object)
        if tipo == "integer":
            df[nome] = pd.to_numeric(coluna).astype("Int64")
        elif tipo == "double precision":
            df[nome] = pd.to_numeric(coluna).astype("float64")
        elif tipo == "boolean":
            df[nome] = coluna.astype("boolean")
        elif tipo == "timestamp":
            df[nome] = pd.to_datetime(coluna)
        else:
            df[nome] = coluna.astype("string")
    return df


def to_copy_buffer(df: pd.DataFrame) -> io.StringIO:
    """CSV para o COPY (sem cabeçalho). Nulo vira `\\N`; texto vazio continua vazio."""
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False, na_rep=NULL_MARK, date_format="%Y-%m-%d %H:%M:%S.%f")
    buffer.seek(0)
    return buffer


def tabelas_divergentes(conn, spec: dict[str, list[tuple[str, str]]]) -> list[str]:
    """Tabelas que já existem no banco com colunas ou tipos diferentes da DDL."""
    linhas = conn.execute(
        text(
            "select table_name, column_name, data_type from information_schema.columns "
            "where table_schema = :schema and table_name = any(:tabelas) "
            "order by table_name, ordinal_position"
        ),
        {"schema": SCHEMA, "tabelas": list(spec)},
    ).fetchall()
    existentes: dict[str, list[tuple[str, str]]] = {}
    for tabela, coluna, tipo in linhas:
        existentes.setdefault(tabela, []).append((coluna, tipo))

    divergentes = []
    for tabela, colunas in spec.items():
        if tabela not in existentes:
            continue  # ainda não existe: a DDL cria
        esperado = [(nome, _TIPOS_PG[tipo]) for nome, tipo in colunas]
        if existentes[tabela] != esperado:
            divergentes.append(tabela)
    return divergentes


def _copy(conn, tabela: str, df: pd.DataFrame) -> None:
    colunas = ", ".join(f'"{c}"' for c in df.columns)
    cursor = conn.connection.cursor()
    try:
        cursor.execute(f"TRUNCATE TABLE {SCHEMA}.{tabela}")
        cursor.copy_expert(
            f"COPY {SCHEMA}.{tabela} ({colunas}) FROM STDIN WITH (FORMAT csv, NULL '{NULL_MARK}')",
            to_copy_buffer(df),
        )
    finally:
        cursor.close()


def load_to_staging():
    logger.info("Iniciando carga de dados da camada Silver para Staging (Postgres-Gold)")
    spec = parse_ddl(DDL_PATH.read_text(encoding="utf-8"))
    if set(spec) != set(SILVER_PATHS):
        raise ValueError(f"A DDL define {sorted(spec)} mas o loader espera {sorted(SILVER_PATHS)}")

    # 1. Lê e valida tudo ANTES de abrir a transação: uma falha no S3 ou um desvio de schema não
    #    deixa nada pela metade nem segura locks no banco.
    carga: dict[str, pd.DataFrame] = {}
    for tabela, caminho in SILVER_PATHS.items():
        df = read_silver(caminho)
        logger.info(f"Lidos {len(df)} registros da tabela {tabela} do S3.")
        validate_columns(tabela, list(df.columns), spec[tabela])
        carga[tabela] = prepare_frame(df, spec[tabela])

    # 2. Uma única transação: ou todas as tabelas são carregadas, ou nenhuma muda.
    engine = create_engine(postgres_url())
    with engine.begin() as conn:
        for tabela in tabelas_divergentes(conn, spec):
            logger.warning(
                f"{SCHEMA}.{tabela} existe com colunas/tipos diferentes da DDL (criada antes do ARQ-03). "
                "Recriando uma única vez; as views stg_* dependentes serão recriadas pelo dbt."
            )
            conn.execute(text(f"DROP TABLE {SCHEMA}.{tabela} CASCADE"))
        conn.exec_driver_sql(DDL_PATH.read_text(encoding="utf-8"))

        for tabela, df in carga.items():
            _copy(conn, tabela, df)
            logger.info(f"[OK] {SCHEMA}.{tabela}: {len(df)} registros (TRUNCATE + COPY).")

    logger.info("Carga de todos os arquivos para staging finalizada com sucesso!")


if __name__ == "__main__":
    load_to_staging()
