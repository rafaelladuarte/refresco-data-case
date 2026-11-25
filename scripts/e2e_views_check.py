"""As views stg_* sobrevivem à carga e enxergam os dados novos sem rodar o dbt (ARQ-03).

    python scripts/e2e_views_check.py <mudancas.json>

Roda logo depois do `load_to_staging` do dia 2 e ANTES do `dbt build`. Se a carga derrubasse as
tabelas (`DROP TABLE ... CASCADE`), as views teriam desaparecido; com `TRUNCATE` + `COPY` elas
continuam lá e já mostram a venda nova do dia 2.
"""

import json
import sys

from sqlalchemy import create_engine, text

from pipeline.common.config import postgres_url

VIEWS = ["stg_vendas", "stg_clientes", "stg_produtos", "stg_financeiro", "stg_clima"]


def main(caminho: str) -> int:
    with open(caminho, encoding="utf-8") as f:
        mudancas = json.load(f)

    engine = create_engine(postgres_url())
    with engine.connect() as conn:
        existentes = {
            r[0]
            for r in conn.execute(
                text("select table_name from information_schema.views where table_schema = 'load_stage'")
            )
        }
        venda_nova = conn.execute(
            text("select count(*) from load_stage.stg_vendas where id_venda = :id"),
            {"id": mudancas["id_venda_nova"]},
        ).scalar_one()

    checks = [
        ("views stg_* existem depois da carga", sorted(set(VIEWS) - existentes), []),
        ("stg_vendas já mostra a venda nova (sem dbt)", venda_nova, 1),
    ]
    falhas = []
    for nome, obtido, esperado in checks:
        ok = obtido == esperado
        print(f"[{'OK' if ok else 'FALHA'}] {nome}: obtido={obtido} esperado={esperado}")
        if not ok:
            falhas.append(nome)
    return 1 if falhas else 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    sys.exit(main(sys.argv[1]))
