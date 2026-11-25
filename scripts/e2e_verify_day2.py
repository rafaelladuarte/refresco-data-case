"""Verificação do "dia 2" do e2e: só as mudanças de e2e_mutate.py podem aparecer.

    python scripts/e2e_verify_day2.py <mudancas.json>

Cada checagem é do tipo "enforçada" (falha o e2e) ou "pendente" (resultado esperado depois de um
card ainda não implementado: aparece como [PENDENTE] e não falha). Com E2E_ENFORCE_PENDING=1 as
pendentes também falham, o que prova que o teste enxerga o problema.
"""

import json
import os
import sys

from sqlalchemy import create_engine, text

from pipeline.common.config import postgres_url
from pipeline.common.storage import quarantine_path, read_silver

# MOD-01 (Sprint 3): o incremental da fato ainda filtra por data_emissao, então perde a venda
# retroativa e a mudança de status. Remover estes nomes quando o MOD-01 for implementado.
PENDENTES = {
    "fato reflete o novo status financeiro",
    "fato contém a venda retroativa",
}


def valor(conn, sql: str, **params):
    return conn.execute(text(sql), params).scalar_one_or_none()


def main(caminho: str) -> int:
    with open(caminho, encoding="utf-8") as f:
        m = json.load(f)
    enforcar_pendentes = os.environ.get("E2E_ENFORCE_PENDING") == "1"
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
        fato_sem_nova = valor(
            conn, "select count(*) from gold.fato_vendas where id_venda <> :id", id=m["id_venda_nova"]
        )

    checks = [
        ("dim_cliente reflete o novo nome", nome, m["nome_cliente_novo"]),
        ("fato reflete o novo status financeiro", status, m["status_novo"]),
        ("fato contém a venda retroativa", retroativa, 1),
        ("demais vendas da fato inalteradas (contagem)", fato_sem_nova, 3482),
        ("quarentena de vendas inalterada", len(read_silver(quarantine_path("vendas", ds))), 18),
        ("quarentena de clientes inalterada", len(read_silver(quarantine_path("clientes", ds))), 5),
    ]

    falhas = []
    for nome_check, obtido, esperado in checks:
        ok = obtido == esperado
        pendente = nome_check in PENDENTES and not enforcar_pendentes
        marca = "OK" if ok else ("PENDENTE" if pendente else "FALHA")
        print(f"[{marca}] {nome_check}: obtido={obtido!r} esperado={esperado!r}")
        if not ok and not pendente:
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
