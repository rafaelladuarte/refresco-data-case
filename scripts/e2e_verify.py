"""Verificação final do e2e: números de referência da carga completa (Sprint 1/2/3).

Lê o Postgres (Gold e auditoria) e a quarentena no MinIO e falha (exit 1) se algum número divergir.
Usa as mesmas variáveis de ambiente do pipeline (POSTGRES_*, AWS_*, AIRFLOW__S3__ENDPOINT_URL) e
E2E_DS (data lógica do dia 1; o run_id das tasks é `e2e-<ds>`).
"""

import os
import sys

from sqlalchemy import create_engine, text

from pipeline.common.config import postgres_url
from pipeline.common.storage import quarantine_path, read_silver

SQL_FATO = "select count(*) from gold.fato_vendas"
SQL_RECEITA = "select round(sum(receita_liquida)::numeric, 2) from gold.fato_vendas"
# DQ-09: produto sem custo não pode ter margem calculada na Q4
SQL_Q4_MARGEM_SEM_CUSTO = """
    select count(*) from gold.vw_q4_margem_bruta
    where custo_disponivel is false and margem_bruta is not null
"""
SQL_AUDITORIA = """
    select entidade, lidas, aprovadas, duplicadas, rejeitadas
    from audit.dq_results where run_id = :run_id and motivo is null
"""
SQL_AUDITORIA_MOTIVOS = """
    select entidade, coalesce(sum(rejeitadas), 0) from audit.dq_results
    where run_id = :run_id and motivo is not null group by entidade
"""

# Auditoria esperada para data/raw: entidade -> (lidas, aprovadas, duplicadas, rejeitadas)
AUDITORIA_ESPERADA = {
    "clientes": (120, 115, 0, 5),
    "produtos": (25, 25, 0, 0),
    "financeiro": (2464, 2454, 0, 10),
    "clima": (2192, 2192, 0, 0),
    "vendas": (3508, 3482, 8, 18),
}


def main() -> int:
    ds = os.environ["E2E_DS"]
    run_id = f"e2e-{ds}"
    engine = create_engine(postgres_url())
    with engine.connect() as conn:
        fato = conn.execute(text(SQL_FATO)).scalar_one()
        receita = conn.execute(text(SQL_RECEITA)).scalar_one()
        margem_sem_custo = conn.execute(text(SQL_Q4_MARGEM_SEM_CUSTO)).scalar_one()
        auditoria = {r[0]: tuple(r[1:]) for r in conn.execute(text(SQL_AUDITORIA), {"run_id": run_id})}
        com_motivo = dict(conn.execute(text(SQL_AUDITORIA_MOTIVOS), {"run_id": run_id}).fetchall())

    # Referência validada na Sprint 1 e mantida (dados de data/raw)
    checks = [
        ("fato_vendas (linhas)", fato, 3482),
        ("quarentena de vendas", len(read_silver(quarantine_path("vendas", ds))), 18),
        ("quarentena de clientes", len(read_silver(quarantine_path("clientes", ds))), 5),
        ("quarentena de financeiro", len(read_silver(quarantine_path("financeiro", ds))), 10),
        ("receita total da fato (R$)", str(receita), "597343.23"),
        ("Q4: linhas sem custo com margem", margem_sem_custo, 0),
    ]
    for entidade, esperado in AUDITORIA_ESPERADA.items():
        obtido = auditoria.get(entidade)
        checks.append(
            (f"auditoria de {entidade} (lidas, aprovadas, duplicadas, rejeitadas)", obtido, esperado)
        )
        # entidades com rejeição precisam detalhar os motivos (pelo menos 1 por registro rejeitado)
        if esperado[3]:
            checks.append(
                (
                    f"auditoria de {entidade}: motivos detalhados",
                    com_motivo.get(entidade, 0) >= esperado[3],
                    True,
                )
            )

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
