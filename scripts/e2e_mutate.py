"""Gera o "dia 2" do e2e: uma cópia de data/raw com três mudanças conhecidas.

    python scripts/e2e_mutate.py <origem_raw> <destino_raw> <mudancas.json>

As linhas alteradas são escolhidas na Gold do dia 1 (já validadas pelo pipeline), então a
mudança nunca cai na quarentena por engano:

  1. status de uma venda antiga e paga muda para CANCELADO (financeiro.csv);
  2. uma venda nova com data retroativa (vendas.csv): `data_emissao` anterior ao máximo já carregado;
  3. um cliente tem o nome alterado (clientes.csv, ISO-8859-1);
  4. o custo do produto P001 muda (produtos.csv): é uma mudança de DIMENSÃO, que não altera nenhuma
     venda e por isso só chega às vendas antigas da fato pela janela de lookback (MOD-01).
"""

import csv
import json
import shutil
import sys
from pathlib import Path

from sqlalchemy import create_engine, text

from pipeline.common.config import postgres_url

ID_PRODUTO_CUSTO = "P001"
CUSTO_NOVO = "2.55"
ID_VENDA_NOVA = "V900001"
DATA_RETROATIVA = "2024-02-10"
SUFIXO_NOME = " Ltda"


def ler(caminho: Path, encoding: str) -> tuple[list[str], list[dict]]:
    with open(caminho, newline="", encoding=encoding) as f:
        leitor = csv.DictReader(f)
        return list(leitor.fieldnames), list(leitor)


def gravar(caminho: Path, encoding: str, colunas: list[str], linhas: list[dict]) -> None:
    with open(caminho, "w", newline="", encoding=encoding) as f:
        escritor = csv.DictWriter(f, fieldnames=colunas, lineterminator="\n")
        escritor.writeheader()
        escritor.writerows(linhas)


def main(origem: str, destino: str, saida: str) -> int:
    origem_dir, destino_dir = Path(origem), Path(destino)
    destino_dir.mkdir(parents=True, exist_ok=True)
    for arquivo in origem_dir.glob("*.csv"):
        shutil.copy(arquivo, destino_dir / arquivo.name)

    engine = create_engine(postgres_url())
    with engine.connect() as conn:
        venda = conn.execute(
            text(
                "select v.id_venda, c.id_cliente, p.id_produto "
                "from gold.fato_vendas v "
                "join gold.dim_cliente c using (sk_cliente) "
                "join gold.dim_produto p using (sk_produto) "
                "where v.status_financeiro = 'PAGO' and v.tipo_lancamento = 'VENDA' "
                "order by v.data_emissao, v.id_venda limit 1"
            )
        ).one()
        cliente = conn.execute(
            text(
                "select id_cliente, nome_cliente from gold.dim_cliente "
                "where nome_cliente ~ '^[A-Za-z ]+$' order by id_cliente limit 1"
            )
        ).one()

    # 1. status financeiro da venda antiga
    colunas, linhas = ler(destino_dir / "financeiro.csv", "utf-8")
    alvo = [linha for linha in linhas if linha["id_venda"] == venda.id_venda]
    assert len(alvo) == 1, f"esperado 1 lançamento para {venda.id_venda}, achei {len(alvo)}"
    status_antigo = alvo[0]["status"]
    alvo[0]["status"] = "CANCELADO"
    gravar(destino_dir / "financeiro.csv", "utf-8", colunas, linhas)

    # 2. venda retroativa nova (copia uma venda válida e troca id e data)
    colunas, linhas = ler(destino_dir / "vendas.csv", "utf-8")
    modelo = next(linha for linha in linhas if linha["id_venda"] == venda.id_venda)
    nova = dict(modelo, id_venda=ID_VENDA_NOVA, data_emissao=DATA_RETROATIVA)
    gravar(destino_dir / "vendas.csv", "utf-8", colunas, [*linhas, nova])

    # 3. nome do cliente (arquivo em ISO-8859-1)
    colunas, linhas = ler(destino_dir / "clientes.csv", "latin1")
    alvo = [linha for linha in linhas if linha["id_cliente"] == cliente.id_cliente]
    assert alvo, f"cliente {cliente.id_cliente} não encontrado no CSV"
    nome_novo = alvo[0]["nome_cliente"].rstrip() + SUFIXO_NOME
    alvo[0]["nome_cliente"] = nome_novo
    gravar(destino_dir / "clientes.csv", "latin1", colunas, linhas)

    # 4. custo do produto (dimensão)
    colunas, linhas = ler(destino_dir / "produtos.csv", "utf-8")
    alvo = [linha for linha in linhas if linha["id_produto"] == ID_PRODUTO_CUSTO]
    assert len(alvo) == 1, f"produto {ID_PRODUTO_CUSTO} não encontrado no CSV"
    custo_antigo = alvo[0]["custo_unitario"]
    alvo[0]["custo_unitario"] = CUSTO_NOVO
    gravar(destino_dir / "produtos.csv", "utf-8", colunas, linhas)

    mudancas = {
        "id_produto_custo": ID_PRODUTO_CUSTO,
        "custo_antigo": custo_antigo,
        "custo_novo": CUSTO_NOVO,
        "id_venda_status": venda.id_venda,
        "status_antigo": status_antigo,
        "status_novo": "CANCELADO",
        "id_venda_nova": ID_VENDA_NOVA,
        "data_venda_nova": DATA_RETROATIVA,
        "id_cliente": cliente.id_cliente,
        "nome_cliente_novo": nome_novo,
    }
    Path(saida).write_text(json.dumps(mudancas, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(mudancas, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    sys.exit(main(*sys.argv[1:]))
