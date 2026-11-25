# 🛠️ Operação e testes

Guia de operação do pipeline: reprocessamento, incremental, quarentena, atualização de ambientes antigos, testes e CI.
O resumo do projeto está no [README](../README.md).

**Sumário:** [Reprocessamento e backfill](#reprocessamento-e-backfill) · [Incremental e lookback](#incremental-da-fato_vendas-e-lookback) · [Quarentena e auditoria](#quarentena-e-auditoria) · [Regras de qualidade](#regras-de-qualidade) · [Atualizando um ambiente existente](#atualizando-um-ambiente-existente) · [Testes](#como-rodar-os-testes)

---

## Reprocessamento e backfill

Cada execução da DAG tem uma **data lógica** (`{{ ds }}`). Bronze e Silver trabalham só com a partição dessa data:

- **Bronze:** grava `bronze/<tabela>/ingestion_date=<ds>/`. Reexecutar a mesma data sobrescreve o mesmo objeto.
- **Silver:** lê a partição da data, valida e faz *merge* com o estado atual por chave natural. Quem vence é a versão de **maior data lógica** (depois, a ingerida por último), então um backfill de uma data antiga nunca desfaz uma data mais nova, mesmo rodando depois. Reexecutar uma data dá o mesmo resultado.
- **Colunas de controle** em toda tabela Silver: `_logical_date`, `_ingested_at`, `_row_hash` (hash do conteúdo de negócio) e `_changed_at` (só avança quando o conteúdo da linha muda).

```bash
# Reprocessar uma data (a DAG tem catchup=False; backfill é explícito)
docker compose exec airflow-scheduler airflow dags backfill dag_etl_pipeline \
  --start-date 2026-11-20 --end-date 2026-11-22

# Uma tarefa avulsa, fora do Airflow, para uma data
python -m pipeline.silver.clean_vendas 2026-11-20
```

## Incremental da `fato_vendas` e lookback

A fato é incremental por **mudança**, não por data. Uma venda é reprocessada (`merge` por `id_venda`) quando:

1. a venda **ou o lançamento financeiro dela** mudou depois da última carga (`atualizado_em`, que vem do `_changed_at` da Silver). Isso cobre vendas retroativas, correções e a mudança de status de uma venda antiga;
2. ela foi emitida nos últimos `lookback_days` dias (variável do dbt, padrão **30**). Isso cobre mudanças nas **dimensões** (como o custo de um produto), que não alteram a venda e por isso não aparecem em `atualizado_em`.

```bash
# janela maior (ex.: depois de corrigir o custo de um produto com vendas antigas)
dbt run --select fato_vendas --vars '{lookback_days: 365}'
# recálculo completo; o `+` recria as views analíticas, que o full-refresh derruba em cascata
dbt run --full-refresh --select fato_vendas+
```

## Quarentena e auditoria

Todo registro rejeitado vai para `silver/_quarentena/<entidade>/<data lógica>` (Parquet, com a coluna `motivo_rejeicao`), para as cinco entidades. Reexecutar uma data substitui só a quarentena dela; as demais datas ficam como histórico.

Cada task Silver também grava em **`audit.dq_results`** (Postgres Gold) o que leu, aprovou, descartou por duplicidade e rejeitou, com uma linha por motivo:

```sql
select entidade, lidas, aprovadas, duplicadas, rejeitadas
from audit.dq_results
where run_id = '<run_id do Airflow>' and motivo is null;   -- resumo; sem o filtro, vem o detalhe por motivo
```

A fonte (`data/raw`) é um *snapshot* completo a cada execução, não uma sequência de deltas. Consequências assumidas: uma linha que **some** da origem permanece na Silver (o merge nunca apaga), e a unicidade de CNPJ entre clientes é aplicada dentro da partição da data, não entre datas.


## Regras de qualidade

| Entidade | Regras na Silver | Quarentena |
|---|---|---|
| Clientes | `id_cliente` no padrão `C9999`; `cnpj` e nome obrigatórios; 1 registro por `id_cliente`; CNPJ duplicado mantém o menor id | `silver/_quarentena/clientes/<data>` |
| Vendas | `id_venda` obrigatório; data válida (4 formatos aceitos); `qtd_caixas > 0`; `preco_unitario > 0`; `0 ≤ desconto_pct < 1`; cliente e produto existentes no cadastro | `silver/_quarentena/vendas/<data>` |
| Produtos | `id_produto` no padrão `P999`; `custo_unitario`/`preco_tabela` `<= 0` ou ausente vira NULL com flag `custo_disponivel`/`preco_disponivel = false` (o produto é mantido e fica fora da margem) | `silver/_quarentena/produtos/<data>` |
| Financeiro | `id_financeiro` obrigatório; competência válida (`YYYYMM` ou `YYYY/MM`); tipos e status mapeados para domínios fixos | `silver/_quarentena/financeiro/<data>` |
| Clima | data (`time`) válida | `silver/_quarentena/clima/<data>` |

Na última execução de referência, **3.482 vendas** chegaram à fato. A quarentena ficou com 18 vendas (6 com quantidade negativa, 3 com desconto ≥ 100%, 5 sem cliente e 4 com produto inválido), 5 recadastros de clientes e 10 lançamentos financeiros com competência inválida (que antes eram descartados em silêncio). Além disso, 8 vendas repetidas na origem foram deduplicadas (vale a mais recente).

### Testes dbt (78)
- Unicidade e `not_null` de chaves.
- Coerência entre flag e valor em `dim_produto`: custo disponível é sempre `> 0`, e indisponível é sempre NULL (idem para preço).
- `relationships` da fato com as dimensões e com o seed de regras: um tipo de lançamento novo sem regra quebra o build.
- Faixas de medidas em `stg_vendas`, como defesa em profundidade.
- Testes dos seeds.
- `accepted_values` em canal, UF, região e família.
- Teste singular `assert_clima_cobre_dias_com_venda`.
- Teste singular `assert_fato_reconcilia_com_staging`: a fato tem a mesma contagem e a mesma soma de `receita_liquida` que `stg_vendas`.

---

## Atualizando um ambiente existente

> **Arquivos do dbt com dono root.** Versões anteriores rodavam o dbt como root, e `dbt/target`, `dbt/logs`, `dbt/dbt_packages` e `dbt/package-lock.yml` podem ter ficado com dono `root`. Para corrigir uma única vez:
> `docker run --rm -v "$PWD/dbt:/w" alpine:3.20 sh -c "chown -R $(id -u):$(id -g) /w"`

> **Atualizando um ambiente existente (custo indisponível).** A partir do DQ-09, P012/P019 têm `custo_total`/`margem_bruta` NULL na `fato_vendas`. Como ela é incremental, as vendas antigas só são recalculadas com `dbt run --full-refresh --select fato_vendas+` (uma vez, depois de rodar a DAG; o `+` recria as views analíticas, que o full-refresh derruba em cascata).

> **Atualizando um ambiente existente (partições Bronze por data).** A partir do ARQ-01 a Bronze é particionada pela data lógica da execução (`ingestion_date=YYYY-MM-DD`), e a Silver processa só a partição da data. As partições antigas, com horário (`ingestion_date=2026-11-25T12:17:55`), são **ignoradas** com um warning. Para removê-las uma única vez (lista primeiro, e só apaga com `--apply`):
> `python scripts/cleanup_legacy_bronze.py` e depois `python scripts/cleanup_legacy_bronze.py --apply` (com as variáveis do `.env` exportadas), ou `docker compose down -v` num ambiente de teste. A Silver gravada antes desta mudança é aceita: as colunas de controle são completadas no primeiro merge.

> **Atualizando um ambiente existente (tabelas de `load_stage`).** A carga agora usa tipos explícitos (`pipeline/ddl/load_stage.sql`) e `TRUNCATE` + `COPY`. Na **primeira** execução depois da atualização, as tabelas de `load_stage` criadas pelo pandas (colunas e tipos diferentes da DDL, como `ano` em `text`) são recriadas, com um aviso, e as views `stg_*` dependentes caem; a `dbt_run_gold` as recria na mesma execução. Daí em diante as views sobrevivem às cargas.

> **Atualizando um ambiente antigo.** Se você já tinha o ambiente rodando antes de as credenciais passarem a vir do `.env`, as credenciais e o volume do MinIO mudaram. Recrie os volumes com `docker compose down -v`. Isso apaga os dados locais, que se recriam rodando a DAG. Se preferir manter os volumes, a `fato_vendas` (incremental) ganhou colunas e precisa de um `dbt run --full-refresh --select fato_vendas+`.


---

## Como rodar os testes

Os testes unitários rodam fora do Docker, em Python 3.12 (`.python-version`). As versões fixadas não têm wheels para o 3.13.

```bash
python -m venv .venv && source .venv/bin/activate   # com pyenv, o 3.12 é selecionado pelo .python-version
pip install -r requirements-dev.txt

pytest --cov                                    # testes + gate de 80% (pipeline/common e funções de regra)
coverage report --include='pipeline/*' --fail-under=0   # relatório completo, sem gate

ruff check . && ruff format --check .           # lint e formato (configuração no pyproject.toml)
```

- `tests/unit/`: funções puras (validação de vendas, dedup de clientes, datas, mapeamentos do financeiro, normalizações, configuração, storage e ingestão de clima com respostas HTTP simuladas). Não precisam de MinIO, Postgres nem rede.
- `tests/integration/test_dag.py`: integridade da DAG (importação, 13 tasks, dependências, `max_active_runs=1`). Precisa do Airflow, então é pulado no virtualenv. Dentro da imagem:

```bash
docker run --rm --entrypoint bash -v "$PWD:/opt/airflow/repo:ro" -w /opt/airflow/repo refresco-data-case-airflow-init \
  -c 'pip install -q --target /tmp/pt pytest==8.3.4 && PYTHONPATH=/tmp/pt python -m pytest tests/integration -p no:cacheprovider'
```

As funções `main()`, que só fazem entrada e saída (S3/Postgres), ficam fora do gate e são cobertas pela execução de ponta a ponta.

### Teste ponta a ponta (`scripts/e2e.sh`)

Roda o pipeline inteiro (Bronze → Silver → `load_stage` → `dbt deps/seed/build`) sobre `data/raw` e compara o resultado com os números de referência (3.482 vendas na fato, 18 e 5 registros nas quarentenas de vendas e clientes, receita total e Q4 sem margem para produtos sem custo). Também confere a auditoria (`audit.dq_results`) e a escrita atômica contra o MinIO real, com falhas injetadas. No fim, roda o teste de integridade da DAG na imagem do Airflow.

```bash
scripts/e2e.sh                       # sobe MinIO e Postgres descartáveis (portas 19000 e 15432) e os remove ao terminar
E2E_DAG_TEST=0 scripts/e2e.sh        # pula o build da imagem do Airflow (mais rápido)
```

Depois do dia 1 ele compara o conteúdo de 31 datasets (Silver, quarentenas, `load_stage` e `gold`) com o baseline versionado em `tests/baseline/fingerprint.json`, o que prova que uma refatoração não mudou dados. Se a mudança de dados for intencional, regrave o baseline com `E2E_UPDATE_BASELINE=1` e justifique no commit. Em seguida roda um **dia 2** (outra data lógica): uma cópia de `data/raw` com quatro mudanças conhecidas (status financeiro de uma venda antiga, venda retroativa, nome de cliente e custo de um produto), conferindo que só elas aparecem, sem `--full-refresh`. Por fim reexecuta o dia 2 (idempotência) e o dia 1 depois do dia 2 (backfill fora de ordem), e exige que o estado fique idêntico.

O script não lê o `.env` nem toca no ambiente de desenvolvimento. O clima vem de respostas gravadas da Open-Meteo em `tests/fixtures/open_meteo/` (variável `CLIMA_FIXTURE_DIR`), então o teste não depende da API.

### CI (GitHub Actions)

`.github/workflows/ci.yml` roda em todo `push` e `pull_request`, com dois jobs:

| Job | O que faz |
|---|---|
| `lint-unit` | `ruff check`, `ruff format --check` e `pytest tests/unit` com o gate de cobertura |
| `e2e` | MinIO e Postgres como *service containers* e `scripts/e2e.sh` |
