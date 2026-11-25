"""Lookback do incremental da fato (MOD-01): mudança de DIMENSÃO em venda fora da janela.

    python scripts/e2e_lookback_check.py <antes|depois> <mudancas.json>

O custo do produto muda no dia 2. Nenhuma venda mudou, então o filtro por `atualizado_em` não as
enxerga, e as vendas de 2024 estão fora da janela de 30 dias contados de hoje:

  antes   depois do `dbt build` padrão, o custo unitário implícito nas vendas antigas continua o ANTIGO
          (limitação conhecida e documentada);
  depois  depois de `dbt run --select fato_vendas --vars '{lookback_days: 100000}'` ele passa a ser o NOVO.
"""

import json
import sys

from sqlalchemy import create_engine, text

from pipeline.common.config import postgres_url

SQL = """
    select coalesce(array_agg(distinct round((f.custo_total / f.qtd_caixas)::numeric, 2)::text), '{}')
    from gold.fato_vendas f join gold.dim_produto p using (sk_produto)
    where p.id_produto = :id_produto and f.id_venda <> :id_venda_nova
"""


def main(fase: str, caminho: str) -> int:
    with open(caminho, encoding="utf-8") as f:
        m = json.load(f)
    parametros = {"id_produto": m["id_produto_custo"], "id_venda_nova": m["id_venda_nova"]}
    with create_engine(postgres_url()).connect() as conn:
        custos = conn.execute(text(SQL), parametros).scalar_one()

    custo_esperado = m["custo_antigo" if fase == "antes" else "custo_novo"]
    esperado = [f"{float(custo_esperado):.2f}"]
    ok = custos == esperado
    produto = m["id_produto_custo"]
    print(
        f"[{'OK' if ok else 'FALHA'}] custo unitário nas vendas de {produto} ({fase} do lookback): "
        f"obtido={custos} esperado={esperado}"
    )
    return 0 if ok else 1


if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[1] not in ("antes", "depois"):
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    sys.exit(main(*sys.argv[1:]))
