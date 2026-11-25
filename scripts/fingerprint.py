"""Fingerprint do conteúdo dos datasets do pipeline, para provar que uma refatoração não mudou dados.

Calcula um hash do conteúdo ordenado de cada dataset (Silver, quarentenas e os schemas
`load_stage` e `gold` do Postgres), ignorando colunas de controle que mudam a cada execução.

    python scripts/fingerprint.py gerar baseline.json     # lê MinIO e Postgres do ambiente
    python scripts/fingerprint.py comparar baseline.json depois.json [--ignorar PREFIXO ...]

Usa as mesmas variáveis de ambiente do pipeline (POSTGRES_*, AWS_*, AIRFLOW__S3__ENDPOINT_URL).
O `scripts/e2e.sh` gera o fingerprint quando E2E_FINGERPRINT_OUT está definida.
"""

import hashlib
import json
import sys

import pandas as pd
from sqlalchemy import create_engine, text

from pipeline.common.config import postgres_url
from pipeline.common.storage import read_silver

# Colunas de controle: mudam a cada execução (relógio, data lógica) ou só existem depois de
# certos cards. O conteúdo de negócio é o que precisa permanecer idêntico.
COLUNAS_VOLATEIS = {
    "_ingested_at",
    "ingested_at",
    "_changed_at",
    "changed_at",
    "atualizado_em",
    "_logical_date",
    "_row_hash",
}

SCHEMAS_POSTGRES = ("load_stage", "gold")

DATASETS_SILVER = {
    "silver/clientes": "s3://silver/clientes/",
    "silver/produtos": "s3://silver/produtos/produtos_clean.parquet",
    "silver/vendas": "s3://silver/vendas/",
    "silver/financeiro": "s3://silver/financeiro/",
    "silver/clima": "s3://silver/clima/",
    "quarentena/clientes": "s3://silver/_quarentena/clientes/",
    "quarentena/produtos": "s3://silver/_quarentena/produtos/",
    "quarentena/financeiro": "s3://silver/_quarentena/financeiro/",
    "quarentena/clima": "s3://silver/_quarentena/clima/",
    "quarentena/vendas": "s3://silver/_quarentena/vendas/",
}


def fingerprint(df: pd.DataFrame) -> dict:
    """Hash do conteúdo, independente da ordem das linhas e das colunas."""
    df = df.drop(columns=[c for c in df.columns if c in COLUNAS_VOLATEIS])
    df = df[sorted(df.columns)].astype(str)
    df = df.sort_values(list(df.columns), kind="mergesort").reset_index(drop=True)
    conteudo = df.to_csv(index=False).encode("utf-8")
    return {"linhas": len(df), "colunas": list(df.columns), "sha256": hashlib.sha256(conteudo).hexdigest()}


def gerar() -> dict:
    resultado = {}
    for nome, caminho in DATASETS_SILVER.items():
        try:
            resultado[nome] = fingerprint(read_silver(caminho))
        except (FileNotFoundError, OSError):
            resultado[nome] = {"ausente": True}

    engine = create_engine(postgres_url())
    with engine.connect() as conn:
        tabelas = conn.execute(
            text(
                "select table_schema, table_name from information_schema.tables "
                "where table_schema = any(:schemas) order by 1, 2"
            ),
            {"schemas": list(SCHEMAS_POSTGRES)},
        ).fetchall()
        for schema, tabela in tabelas:
            df = pd.read_sql(text(f'select * from "{schema}"."{tabela}"'), conn)
            resultado[f"{schema}.{tabela}"] = fingerprint(df)
    return resultado


def comparar(a: dict, b: dict, ignorar: tuple[str, ...] = ()) -> int:
    """Compara dois fingerprints. `ignorar` descarta datasets cujo nome começa com algum prefixo."""
    a = {k: v for k, v in a.items() if not k.startswith(ignorar)}
    b = {k: v for k, v in b.items() if not k.startswith(ignorar)}
    diferentes = []
    for nome in sorted(set(a) | set(b)):
        if nome not in a:
            diferentes.append((nome, "só no segundo"))
        elif nome not in b:
            diferentes.append((nome, "só no primeiro"))
        elif a[nome] != b[nome]:
            diferentes.append((nome, f"{a[nome].get('linhas')} -> {b[nome].get('linhas')} linhas"))

    iguais = len(set(a) | set(b)) - len(diferentes)
    print(f"{iguais} datasets idênticos, {len(diferentes)} diferentes")
    for nome, motivo in diferentes:
        print(f"  [DIFERENTE] {nome}: {motivo}")
    return 1 if diferentes else 0


def main(argv: list[str]) -> int:
    if len(argv) == 3 and argv[1] == "gerar":
        resultado = gerar()
        with open(argv[2], "w", encoding="utf-8") as f:
            json.dump(resultado, f, indent=2, sort_keys=True)
        print(f"{len(resultado)} datasets gravados em {argv[2]}")
        return 0
    if len(argv) >= 4 and argv[1] == "comparar":
        resto = argv[4:]
        ignorar = tuple(
            resto[i + 1] for i, arg in enumerate(resto) if arg == "--ignorar" and i + 1 < len(resto)
        )
        with open(argv[2], encoding="utf-8") as f1, open(argv[3], encoding="utf-8") as f2:
            return comparar(json.load(f1), json.load(f2), ignorar)
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
