"""Verificação final do e2e: números de referência da carga completa (Sprint 1/2).

Lê o Postgres (Gold) e a quarentena no MinIO e falha (exit 1) se algum número divergir.
Usa as mesmas variáveis de ambiente do pipeline (POSTGRES_*, AWS_*, AIRFLOW__S3__ENDPOINT_URL).
"""

import sys

from sqlalchemy import create_engine, text

from pipeline.common.config import postgres_url
from pipeline.common.storage import read_silver

SQL_FATO = "select count(*) from gold.fato_vendas"
SQL_RECEITA = "select round(sum(receita_liquida)::numeric, 2) from gold.fato_vendas"
# DQ-09: produto sem custo não pode ter margem calculada na Q4
SQL_Q4_MARGEM_SEM_CUSTO = """
    select count(*) from gold.vw_q4_margem_bruta
    where custo_disponivel is false and margem_bruta is not null
"""


def main() -> int:
    engine = create_engine(postgres_url())
    with engine.connect() as conn:
        fato = conn.execute(text(SQL_FATO)).scalar_one()
        receita = conn.execute(text(SQL_RECEITA)).scalar_one()
        margem_sem_custo = conn.execute(text(SQL_Q4_MARGEM_SEM_CUSTO)).scalar_one()

    # Referência validada na Sprint 1 e mantida na Sprint 2 (dados de data/raw)
    checks = [
        ("fato_vendas (linhas)", fato, 3482),
        ("quarentena de vendas", len(read_silver("s3://silver/_quarentena/vendas/")), 18),
        ("quarentena de clientes", len(read_silver("s3://silver/_quarentena/clientes/")), 5),
        ("receita total da fato (R$)", str(receita), "597343.23"),
        ("Q4: linhas sem custo com margem", margem_sem_custo, 0),
    ]

    falhas = []
    for nome, obtido, esperado in checks:
        ok = obtido == esperado
        print(f"[{'OK' if ok else 'FALHA'}] {nome}: obtido={obtido} esperado={esperado}")
        if not ok:
            falhas.append(nome)

    if falhas:
        print(f"\nVerificação reprovada: {', '.join(falhas)}", file=sys.stderr)
        return 1
    print("\nVerificação aprovada.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
