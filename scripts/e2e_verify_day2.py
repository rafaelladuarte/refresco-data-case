"""Verificação do "dia 2" do e2e: só as mudanças de e2e_mutate.py podem aparecer.

    python scripts/e2e_verify_day2.py <mudancas.json>

Toda checagem reprova o e2e se divergir. As duas que acusavam o bug do incremental (status financeiro
e venda retroativa fora da fato) ficaram pendentes até o MOD-01 e agora são exigidas.
"""

import json
import os
import sys

from sqlalchemy import create_engine, text

from pipeline.common.config import postgres_url
from pipeline.common.storage import quarantine_path, read_silver


def valor(conn, sql: str, **params):
    return conn.execute(text(sql), params).scalar_one_or_none()


def main(caminho: str) -> int:
    with open(caminho, encoding="utf-8") as f:
        m = json.load(f)
    ds = os.environ["E2E_DS"]  # data lógica do dia 2 (a quarentena é por data)

    engine = create_engine(postgres_url())
    with engine.connect() as conn:
        status = valor(
            conn,
            "select status_financeiro from gold.fato_vendas where id_venda = :id",
            id=m["id_venda_status"],
        )
        retroativa = valor(
            conn, "select count(*) from gold.fato_vendas where id_venda = :id", id=m["id_venda_nova"]
        )
        nome = valor(
            conn, "select nome_cliente from gold.dim_cliente where id_cliente = :id", id=m["id_cliente"]
        )
        fato_total = valor(conn, "select count(*) from gold.fato_vendas")
        fato_sem_nova = valor(
            conn, "select count(*) from gold.fato_vendas where id_venda <> :id", id=m["id_venda_nova"]
        )

    checks = [
        ("dim_cliente reflete o novo nome", nome, m["nome_cliente_novo"]),
        ("fato reflete o novo status financeiro", status, m["status_novo"]),
        ("fato contém a venda retroativa", retroativa, 1),
        ("demais vendas da fato inalteradas (contagem)", fato_sem_nova, 3482),
        ("fato = stg_vendas (reconciliação, com a venda nova)", fato_total, 3483),
        ("quarentena de vendas inalterada", len(read_silver(quarantine_path("vendas", ds))), 18),
        ("quarentena de clientes inalterada", len(read_silver(quarantine_path("clientes", ds))), 5),
    ]

    falhas = []
    for nome_check, obtido, esperado in checks:
        ok = obtido == esperado
        print(f"[{'OK' if ok else 'FALHA'}] {nome_check}: obtido={obtido!r} esperado={esperado!r}")
        if not ok:
            falhas.append(nome_check)

    if falhas:
        print(f"\nDia 2 reprovado: {', '.join(falhas)}", file=sys.stderr)
        return 1
    print("\nDia 2 aprovado.")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    sys.exit(main(sys.argv[1]))
