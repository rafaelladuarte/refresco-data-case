# 📋 Backlog — Refresco Data-Case

Quadro Kanban com as melhorias e evoluções do projeto, levantadas na revisão técnica de 24/11/2026 e organizadas em sprints.
Para mover um card, recorte-o e cole na coluna correspondente. Marque os critérios de aceite conforme forem concluídos.

## Legenda

| Campo | Valores |
|---|---|
| **Prioridade** | 🔴 Alta · 🟠 Média · 🟢 Baixa |
| **Complexidade** | `P` (até 2h) · `M` (meio dia a 1 dia) · `G` (vários dias) |
| **Tipo** | `bug` · `qualidade-dados` · `infra` · `eng-software` · `modelagem` · `observabilidade` · `docs` · `evolução` |
| **Prefixo do ID** | `DQ` qualidade de dados · `ARQ` arquitetura/pipeline · `ENG` engenharia de software · `INF` infraestrutura · `MOD` modelagem · `OBS` observabilidade · `DOC` documentação · `EVO` evolução opcional |

## Visão geral do quadro

| Coluna | Cards | Foco |
|---|---|---|
| 🔜 A Fazer — Sprint 2 | 6 | Fundação de engenharia: módulo comum, testes, CI, contratos dbt |
| 📦 Sprint 3 | 6 | Pipeline robusto: idempotência, atomicidade, incremental |
| 📦 Sprint 4 | 7 | Modelagem e observabilidade |
| 🧊 Opcionais | 4 | Diferenciais de portfólio |
| 🚧 Em Andamento | 0 | |
| 👀 Em Revisão | 0 | |
| ✅ Concluído | 13 + 1 extra | Sprint 1 (24/11/2026) |

**Roadmap:** Sprint 1 ✅ → **Sprint 2** (fundação) → Sprint 3 (robustez) → Sprint 4 (modelagem/observabilidade) → opcionais.
A ordem importa: a Sprint 3 muda partes centrais do pipeline (escrita, carga, incremental), e os testes e a CI da Sprint 2 é que permitem fazer isso com segurança.

---

## 🔜 A Fazer — Sprint 2 · Fundação de engenharia

> **Objetivo:** criar a rede de segurança (testes, CI, contratos mais rígidos) antes das mudanças estruturais da Sprint 3, e fechar os achados da validação da Sprint 1.
> **Plano:** [`docs/backlog/SPRINT-02.md`](docs/backlog/SPRINT-02.md) (aprovado em 24/11/2026)

### ENG-01 · Criar módulo compartilhado `pipeline/common/`
`eng-software` · 🔴 Alta · `M`

A Sprint 1 já centralizou as credenciais (`pipeline/settings.py`) e a limpeza de texto (`pipeline/silver/text.py`). Continuam duplicados: criação do cliente S3 (7 arquivos), `logging.basicConfig` (8), leitura da Bronze por glob, `rm` + `to_parquet` da Silver e quarentena (5), `ensure_bucket_exists` (2) e regex de ID (2).

- **Critérios de aceite:**
  - [ ] `pipeline/common/config.py`: configuração tipada (absorve `settings.py`)
  - [ ] `pipeline/common/storage.py`: cliente S3, leitura da Bronze, escrita da Silver e da quarentena
  - [ ] `pipeline/common/validators.py`: regex de IDs, `normalize_id`, `clean_text`
  - [ ] `pipeline/common/logging.py`: configuração única de logging
  - [ ] Scripts Bronze/Silver/Load usando o módulo comum, sem mudança de comportamento (validado pelo e2e)
  - [ ] Pacote vazio `pipeline/gold/` removido

### ENG-02 · Testes unitários da camada Silver
`eng-software` · 🔴 Alta · `M`

- **Critérios de aceite:**
  - [ ] `tests/` com pytest e fixtures pequenas de DataFrame
  - [ ] Cobertura de `validate_vendas`, `deduplicate_clientes`, `parse_mixed_dates`, `normalize_ativo`, mapeamentos de `tipo_lancamento`/`status`
  - [ ] Casos de borda: nulos, espaços, IDs fora do padrão, datas BR/ISO, descontos inválidos
  - [ ] Teste de importação da DAG (DAG carrega sem erros e sem ciclos)

### ENG-03 · Pipeline de CI no GitHub Actions
`eng-software` · 🔴 Alta · `M`

- **Critérios de aceite:**
  - [ ] Lint e format com `ruff` (configuração versionada no `pyproject.toml`)
  - [ ] `pytest` com relatório de cobertura
  - [ ] Job e2e: MinIO + Postgres como service containers, pipeline Python sobre `data/raw` e `dbt build` (seeds, modelos e testes)
  - [ ] Badge de status no README

### MOD-02 · Endurecer os testes dbt
`qualidade-dados` · 🟠 Média · `P`

Os órfãos já são removidos na Silver, então os testes de relacionamento com `warn` perderam o motivo.

- **Arquivos:** `dbt/models/marts/gold/schema.yml`
- **Critérios de aceite:**
  - [ ] `relationships` de `sk_cliente` e `sk_produto` com `severity: error`
  - [ ] Teste de unicidade de `id_venda` em `stg_financeiro` (protege a fato contra fan-out)
  - [ ] Teste singular de reconciliação: contagem e soma de `receita_liquida` na fato iguais às de `stg_vendas`
  - [ ] `accepted_values` para `canal`, `uf`, `familia`

### DQ-09 · Validar custo unitário de produtos
`qualidade-dados` · 🟠 Média · `P`

_Achado na validação final da Sprint 1._ `P012` (Del Valle Pêssego) e `P019` (Burn Energy) chegam com `custo_unitario = 0.00` na origem e aparecem na Q4 com margem de 100%. Nenhuma regra valida custo hoje.

- **Critérios de aceite:**
  - [ ] Regra na Silver de produtos: `custo_unitario > 0` e `preco_tabela > 0` (ou custo zero sinalizado explicitamente)
  - [ ] Produto inválido vai para quarentena, ou é mantido com flag `custo_indisponivel` e excluído da margem (decisão de negócio)
  - [ ] Teste dbt em `dim_produto.custo_unitario`

### INF-04 · Artefatos do dbt criados como root
`infra` · 🟢 Baixa · `P`

_Achado na Sprint 1._ O container dbt roda como root, então `dbt/logs`, `dbt/target` e `dbt/dbt_packages` ficam com dono `root` no host e atrapalham quem roda o dbt fora do Docker.

- **Critérios de aceite:**
  - [x] DockerOperator executa o dbt com o UID/GID do host (`user=`) ou os artefatos vão para um volume dedicado
  - [x] Documentado no README

---

## 📦 Backlog — Sprint 3 · Pipeline robusto

> **Objetivo:** atacar as causas raiz dos problemas vistos na validação final: execução idempotente, escrita atômica, carga tipada, incremental correto e auditoria de qualidade.

### ARQ-01 · Ingestão idempotente por `logical_date`
`evolução` · 🔴 Alta · `G`

Hoje cada execução grava uma partição com `datetime.now()` e a Silver relê `bronze/**` inteira: o custo cresce sem limite e não há backfill.

- **Critérios de aceite:**
  - [ ] Partição Bronze baseada em `{{ ds }}` / `logical_date`
  - [ ] Reexecutar o mesmo dia sobrescreve a mesma partição (idempotente)
  - [ ] Silver processa apenas as partições novas e faz merge com o estado atual
  - [ ] Backfill de um período testado e documentado

### ARQ-02 · Escrita atômica na Silver
`eng-software` · 🟠 Média · `M`

O padrão `fs.rm()` seguido de `to_parquet()` deixa a Silver vazia ou parcial se a escrita falhar.

- **Critérios de aceite:**
  - [ ] Escrita em prefixo temporário (`_tmp/<run_id>/`) e troca no final
  - [ ] Falha no meio da escrita preserva a versão anterior

### ARQ-03 · Carga em `load_stage` com DDL explícita e `COPY`
`eng-software` · 🟠 Média · `M`

`DROP TABLE ... CASCADE` derruba as views `stg_*` a cada carga, e os tipos ficam a cargo do pandas (`ano` virou `text`).

- **Arquivos:** `pipeline/load/staging.py:59-64`
- **Critérios de aceite:**
  - [ ] DDL versionada para as tabelas de `load_stage`
  - [ ] Carga com `TRUNCATE` + `COPY` numa transação
  - [ ] Views `stg_*` sobrevivem à carga (`dbt_run_staging` deixa de ser obrigatório a cada execução)

### MOD-01 · Corrigir o incremental da `fato_vendas`
`modelagem` · 🔴 Alta · `M`

O filtro `data_emissao > max(data_emissao)` perde vendas retroativas e atualizações de status financeiro.

- **Arquivos:** `dbt/models/marts/gold/fato_vendas.sql:1-13`
- **Critérios de aceite:**
  - [ ] `incremental_strategy='merge'` (ou `delete+insert`) com janela de lookback configurável (`var('lookback_days', 30)`)
  - [ ] Filtro por `ingested_at` em vez de `data_emissao`
  - [ ] Teste: alterar o status de uma venda antiga e confirmar a propagação

### ARQ-04 · Concentrar regras de negócio no dbt
`modelagem` · 🟢 Baixa · `P`

`receita_liquida` é calculada no pandas e `margem_bruta` no dbt.

- **Critérios de aceite:**
  - [ ] Silver só limpa e tipa; todas as métricas são calculadas no dbt
  - [ ] Fórmulas documentadas no `schema.yml`

### ARQ-05 · Quarentena genérica e tabela de auditoria
`qualidade-dados` · 🟠 Média · `M`

Hoje só `vendas` grava quarentena; clientes e produtos só fazem log.

- **Critérios de aceite:**
  - [ ] Quarentena para todas as entidades em `silver/_quarentena/<entidade>/<data>/`
  - [ ] Tabela `audit.dq_results` (run_id, entidade, lidas, aprovadas, rejeitadas, motivo, timestamp)
  - [ ] Cada task Silver grava sua linha de auditoria

---

## 📦 Backlog — Sprint 4 · Modelagem e observabilidade

> **Objetivo:** evoluir o modelo dimensional (grão financeiro, histórico, representantes) e tornar o pipeline observável.

### MOD-03 · Revisar `dim_representante`
`modelagem` · 🟠 Média · `M`

A dimensão é derivada de vendas por (nome, canal), então o mesmo representante aparece uma vez por canal e não se relaciona com `dim_cliente.representante`.

- **Critérios de aceite:**
  - [ ] Uma linha por representante (chave pelo nome normalizado ou por um ID criado)
  - [ ] Canal como atributo da fato, não da dimensão de representante
  - [ ] Relação com o representante da carteira do cliente documentada

### MOD-04 · Fato de lançamentos financeiros
`modelagem` · 🟠 Média · `M`

Os valores financeiros são descartados na Silver e o grão financeiro está misturado na fato de vendas.

- **Critérios de aceite:**
  - [ ] Silver mantém `valor_bruto`, `valor_imposto`, `valor_liquido`
  - [ ] `fato_lancamento_financeiro` (grão: lançamento) ligada a vendas e ao tempo
  - [ ] Views analíticas usam a fato certa para cada pergunta

### MOD-05 · SCD Tipo 2 para clientes com dbt snapshots
`modelagem` · 🟢 Baixa · `M`

As dimensões se dizem SCD1, mas o histórico de canal e representante do cliente se perde.

- **Critérios de aceite:**
  - [ ] `dbt/snapshots/snap_clientes.sql` (estratégia `check` em canal, representante, ativo)
  - [ ] `dim_cliente` com `valid_from`, `valid_to`, `is_current`
  - [ ] Fato com join pela versão vigente na data da venda

### OBS-01 · Alertas e callbacks no Airflow
`observabilidade` · 🟠 Média · `P`

- **Critérios de aceite:**
  - [ ] `on_failure_callback` (log estruturado e notificação opcional por webhook)
  - [ ] SLA configurado nas tasks críticas
  - [ ] `retries` e `retry_delay` ajustados por tipo de task (API vs. transformação)

### OBS-02 · Persistir resultados do dbt e freshness
`observabilidade` · 🟢 Baixa · `M`

- **Critérios de aceite:**
  - [ ] `run_results.json` processado em `audit.dbt_runs`
  - [ ] `dbt source freshness` configurado para `load_stage`
  - [ ] `dbt docs` publicado (GitHub Pages)

### OBS-03 · Página de qualidade de dados no Streamlit
`observabilidade` · 🟢 Baixa · `M`

- **Critérios de aceite:**
  - [ ] Página lendo `audit.dq_results` e `audit.dbt_runs`
  - [ ] Taxa de rejeição por entidade e por motivo ao longo do tempo
  - [ ] Status da última execução do pipeline

### ENG-04 · Contratos de dados
`eng-software` · 🟢 Baixa · `M`

- **Critérios de aceite:**
  - [ ] Schemas Pandera por entidade na Silver (tipos, faixas, regex), substituindo a validação imperativa
  - [ ] `contract: {enforced: true}` nos modelos da Gold
  - [ ] Violação de contrato gera quarentena (Silver) ou falha de build (Gold)

---

## 🧊 Opcionais · conforme o foco do portfólio

> Itens de diferenciação. Puxar apenas se fizerem sentido para a vaga ou o foco desejado (dados, analytics ou backend).

### EVO-01 · Astronomer Cosmos para tasks dbt por modelo *(opcional)*
`evolução` · 🟢 Baixa · `M`

Dá visibilidade e retry por modelo no Airflow. Só vale depois de ARQ-01 e MOD-01.

- [ ] DAG gerada pelo Cosmos com DockerOperator/virtualenv
- [ ] Grafo do dbt refletido no Airflow

### EVO-02 · API FastAPI read-only sobre a Gold *(opcional)*
`evolução` · 🟢 Baixa · `G`

Cobre a parte de backend do portfólio. Não é necessária para o pipeline.

- [ ] Endpoints de métricas por canal, produto e período, com filtros e paginação
- [ ] Schemas Pydantic, OpenAPI documentado, testes com `TestClient`
- [ ] Container próprio no compose; Streamlit pode consumir a API

### EVO-03 · Formato de tabela na Silver (Delta Lake) *(opcional)*
`evolução` · 🟢 Baixa · `G`

Resolve a atomicidade (ARQ-02) e acrescenta time travel e merge nativo, sem Spark (lib `deltalake`).

- [ ] Silver gravada em Delta no MinIO
- [ ] Merge incremental usando `MERGE` do Delta
- [ ] Leitura no load via `deltalake`/`pyarrow`

> ⚠️ **Fora de escopo de propósito:** Spark, Kafka e Kubernetes. Com ~3,5 mil linhas, seria tecnologia sem justificativa.

### DQ-10 · Clima por região
`evolução` · 🟢 Baixa · `M`

Os clientes estão em 4 mesorregiões de MG, mas o clima vem de uma única coordenada (Uberlândia).

- [ ] Coordenada representativa por região (seed `regioes_coordenadas`)
- [ ] Ingestão de clima por região e join por (região do cliente, data)

> ⚠️ **Fora de escopo de propósito:** Spark, Kafka e Kubernetes. Com ~3,5 mil linhas, seria tecnologia sem justificativa.

---

## 🚧 Em Andamento

_Nenhum card._

---

## 👀 Em Revisão

_Nenhum card._

---

## ✅ Concluído

> **Sprint 1** — concluída em 24/11/2026. Plano, decisões e resultados: [`docs/backlog/SPRINT-01.md`](docs/backlog/SPRINT-01.md).

### DQ-01 · Corrigir encoding na ingestão Bronze
`bug` · 🔴 Alta · `P`

Todos os CSVs são lidos como `latin1`, mas só `clientes.csv` é ISO-8859; `vendas`, `produtos` e `financeiro` são UTF-8. Resultado: `canal_venda` chega como `ConveniÃªncia` na fato e na `dim_representante`.

- **Arquivos:** `pipeline/bronze/ingest_csv.py:62`, `pipeline/silver/clean_produtos.py:13`, `pipeline/silver/clean_financeiro.py:10`
- **Critérios de aceite:**
  - [x] Encoding definido por arquivo (mapa de configuração) ou detectado automaticamente
  - [x] Bronze gravada sempre em UTF-8
  - [x] Funções `fix_encoding` removidas da Silver
  - [x] `select distinct canal_venda from gold.dim_representante` sem caracteres corrompidos — _validado na Gold na validação final (0 valores corrompidos)_

### DQ-02 · Corrigir coordenadas da API de clima
`bug` · 🔴 Alta · `P`

`-21.78, -43.35` é Juiz de Fora. Uberlândia ≈ `-18.92, -48.28`.

- **Arquivos:** `pipeline/bronze/ingest_clima.py:12-13`, `:45-46`, `:79`
- **Critérios de aceite:**
  - [x] Latitude/longitude e período parametrizados (env/config), não hardcoded
  - [x] Período derivado das datas de venda (ou do `logical_date`), não `range(2020, 2026)` fixo — _conforme o plano aprovado: período via `CLIMA_ANO_INICIO`/`CLIMA_ANO_FIM` (sem `range` fixo); derivação pelo `logical_date` fica no ARQ-01_
  - [x] Constantes mortas removidas (`API_URL`, `OPEN_METEO_API_URL` do `.env.example`)

### DQ-03 · Eliminar o valor `"nan"` como string
`bug` · 🔴 Alta · `P`

`astype(str)` transforma `NaN` em `"nan"`, o que torna os filtros `notna()` código morto e obriga a view Q2 a ter `WHEN c.canal = 'nan'`.

- **Arquivos:** `pipeline/silver/clean_clientes.py:55,73-74`, `clean_produtos.py:43`, `clean_financeiro.py:42,57`, `dbt/models/marts/analytics/vw_q2_volume_vs_meta.sql:6`
- **Critérios de aceite:**
  - [x] Uso de `.astype("string")` (dtype nullable) ou tratamento explícito de nulos
  - [x] Nenhum valor literal `'nan'` em `load_stage.*`
  - [x] Remendo removido da `vw_q2`

### DQ-04 · Validar medidas de vendas (desconto, quantidade, preço)
`qualidade-dados` · 🔴 Alta · `M`

`desconto_pct` chega a **20.73** (deveria ser `< 1`) e há 6 vendas com `qtd_caixas <= 0`, o que gera `receita_liquida` negativa. A `vw_q4` esconde isso com `receita_liquida > 0`.

- **Arquivos:** `pipeline/silver/clean_vendas.py:144-148`, `dbt/models/marts/analytics/vw_q4_margem_bruta.sql:24`
- **Critérios de aceite:**
  - [x] Regras: `0 <= desconto_pct < 1`, `qtd_caixas > 0`, `preco_unitario > 0`
  - [x] Registros inválidos vão para a quarentena com `motivo_rejeicao`
  - [x] Filtro `receita_liquida > 0` removido da `vw_q4`
  - [x] Testes dbt (`dbt_utils.accepted_range`) nas colunas de `stg_vendas`
  - [x] _(escopo ampliado)_ Datas em `DD/MM/AA` e `DD-MM-AAAA` reconhecidas; data inválida vai para a quarentena em vez de ser descartada em silêncio

### DQ-05 · Substituir meta circular da Q2 por seed de metas
`bug` · 🔴 Alta · `P`

`meta = realizado × fator`, então o atingimento é sempre `1/fator` (≈ 90,9%). A métrica não informa nada.

- **Arquivos:** `dbt/models/marts/analytics/vw_q2_volume_vs_meta.sql:19-28`
- **Critérios de aceite:**
  - [x] `dbt/seeds/metas_canal_mes.csv` (canal, ano, mes, meta_volume_caixas)
  - [x] `vw_q2` faz join com `ref('metas_canal_mes')`
  - [x] Seed documentado e testado (`not_null`, `unique` na combinação canal+ano+mes)

### DQ-06 · Considerar `tipo_lancamento` nas views analíticas
`qualidade-dados` · 🟠 Média · `P`

As views filtram só por `status = 'PAGO'`, então devoluções e bonificações contam como receita.

- **Arquivos:** `vw_q1_ranking_vendas.sql`, `vw_q3_tendencia_volume.sql`, `vw_q4_margem_bruta.sql`
- **Critérios de aceite:**
  - [x] Regra de negócio definida e documentada (ex.: só `VENDA` entra em receita; `DEVOLUCAO` é subtraída)
  - [x] Regra aplicada de forma consistente nas 4 views

### DQ-07 · Clima no grão diário na `dim_tempo`
`modelagem` · 🟠 Média · `P`

O join é por `(ano, mes)`, então cada dia carrega a média e a soma de precipitação do mês inteiro.

- **Arquivos:** `dbt/models/marts/gold/dim_tempo.sql:47-77`
- **Critérios de aceite:**
  - [x] Join por `data_completa = data_clima::date`
  - [x] Agregados mensais, se necessários, em colunas próprias (`clima_temp_max_media_mes`)
  - [x] Comentário desatualizado ("apenas 2024") corrigido, assim como os limites `warn_if`/`error_if` no `schema.yml`
  - [x] _(extra)_ Teste singular `assert_clima_cobre_dias_com_venda`: falha se algum dia com venda ficar sem clima

### DQ-08 · Deduplicar clientes pela chave natural
`bug` · 🔴 Alta · `P`

`clean_clientes` deduplica por `cnpj` (há 5 CNPJs duplicados), não por `id_cliente`.

> Atualizado na Sprint 1: o dedup por CNPJ mantinha os recadastros sem vendas (C0200–C0204) e descartava os originais (C0001–C0005), mandando as vendas deles para a quarentena como órfãs. Além disso, o empate em `_ingested_at` fazia o resultado variar entre execuções.

- **Arquivos:** `pipeline/silver/clean_clientes.py:76-77`
- **Critérios de aceite:**
  - [x] Dedup por `id_cliente` (mais recente por `_ingested_at`)
  - [x] CNPJs duplicados entre clientes distintos registrados na quarentena/log como alerta

### INF-01 · Fixar versões de dependências e imagens
`infra` · 🔴 Alta · `P`

`requirements.txt` não fixa versões e inclui `apache-airflow` dentro da imagem do Airflow, o que pode trocar a versão no build. Há versões inconsistentes entre Pipfile (3.13), imagem (3.12), `.env.example` (2.7.1) e Dockerfile (2.9.0), além de tags `:latest`.

- **Arquivos:** `requirements.txt`, `Dockerfile`, `Pipfile`, `.env.example`, `docker-compose.yml`
- **Critérios de aceite:**
  - [x] `apache-airflow` removido do `requirements.txt`; instalação com o constraints file oficial
  - [x] Versões fixadas (`pandas==`, `s3fs==`, …)
  - [x] `pytest` movido para dependências de desenvolvimento
  - [x] Tags explícitas para MinIO, pgAdmin e dbt (`dbt-postgres:1.8.x`)
  - [x] Uma única fonte de verdade para a versão do Python e do Airflow

### INF-02 · Remover segredos e credenciais hardcoded
`infra` · 🟠 Média · `P`

- **Arquivos:** `docker-compose.yml:15` (Fernet key), `:41-43`, `:77-78`, `:91-92`, `:115-121`; defaults `"minio123"` em todos os scripts
- **Critérios de aceite:**
  - [x] Fernet key, senhas do Airflow/pgAdmin/MinIO vindas do `.env`
  - [x] Scripts falham explicitamente sem credencial (sem default de senha)
  - [x] `.env.example` com todas as variáveis necessárias

### INF-03 · Limpar serviços e artefatos sem uso
`infra` · 🟢 Baixa · `P`

- **Itens:**
  - [x] Remover o serviço `dbt-core` (`tail -f /dev/null`), já que o dbt roda via DockerOperator
  - [x] Remover `init/01_schema.sql` (tabela `gold_vendas` não utilizada)
  - [x] Montar `docker.sock` como read-only ou ajustar o README — _decidido manter leitura/escrita (`:ro` não restringe a API); risco documentado no compose, README e `docs/arquitetura_dados.md` atualizados no DOC-01_
  - [x] Parametrizar o GID `984` (`DOCKER_GID` no `.env`)

### DOC-01 · Alinhar README com o código
`docs` · 🔴 Alta · `M`

O README descreve funcionalidades que não existem. Para quem avalia, isso pesa mais do que a falta de uma funcionalidade.

- **Critérios de aceite:**
  - [x] Diagrama ER com as colunas reais (sem `id_representante` nem `receita_bruta`)
  - [x] Quarentena descrita como está implementada (ou depois do ARQ-05)
  - [x] Passo `airflow db init` removido (o `airflow-init` já faz a migração)
  - [x] Estrutura de pastas atualizada (sem `perguntas.sql`, com `streamlit/` e `notebooks/`)
  - [x] Seção "Limitações conhecidas" e link para este `BACKLOG.md`

### DOC-02 · Atualizar documentos de decisão
`docs` · 🟢 Baixa · `P`

- **Critérios de aceite:**
  - [x] `docs/decisoes_tratamento.md` preenchido (formato ADR: contexto, decisão, consequências) ou removido
  - [x] `docs/diagnostico_orfaos_silver.md` marcado como **resolvido**, com referência ao commit da correção
  - [x] Comentário da DAG (`dags/etl_pipeline.py:125-130`) corrigido: as views somem por causa do `DROP ... CASCADE`, não do container efêmero

### ✔ Extra · `max_active_runs=1` e dependências do Streamlit
`bug` · achados na validação final

- [x] DAG com `max_active_runs=1`: execuções simultâneas misturavam arquivos na Silver e duplicavam a Gold
- [x] `numpy`/`pyarrow` fixados no Streamlit (Q4 quebrava com `ImportError`) e rótulos dos gráficos Q1/Q4 sem corte
