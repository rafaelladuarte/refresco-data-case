# Sprint 2: Fundação de engenharia

> **Status:** 🟢 Aprovado em 24/11/2026 (todas as decisões conforme as recomendações) · em execução
> **Escopo:** os 6 cards da coluna "A Fazer — Sprint 2" do [`BACKLOG.md`](../../BACKLOG.md): ENG-01, ENG-02, ENG-03, MOD-02, DQ-09 e INF-04
> **Objetivo:** criar a rede de segurança (módulo comum, testes unitários, CI de ponta a ponta e contratos dbt mais rígidos) antes das mudanças estruturais da Sprint 3, e fechar os achados da validação da Sprint 1.

**Por que esta sprint vem antes da Sprint 3.** A validação da Sprint 1 só pegou a corrupção por execuções simultâneas e a quebra da Q4 porque a aplicação foi rodada inteira. A Sprint 3 vai mexer em escrita, carga e incremental. Sem testes automatizados e sem CI, cada mudança dependeria de outra validação manual.

---

## 1. Antes de aprovar: decisões pendentes

### D1 · Produtos com custo zero (DQ-09)

`P012` (Del Valle Pêssego) e `P019` (Burn Energy) chegam com `custo_unitario = 0.00` e aparecem na Q4 com margem de 100%.

| Opção | Comportamento | Prós e contras |
|---|---|---|
| **A (recomendada)** | Produto mantido. Custo `<= 0` vira `NULL` na Silver e ganha a flag `custo_disponivel = false`, registrada no log. A fato fica com `custo_total`/`margem_bruta` nulos para esses produtos, e a Q4 os mostra como "custo indisponível", fora do ranking de margem. | As vendas continuam contando em receita e volume. A margem deixa de ser inflada, e o problema fica visível em vez de escondido. |
| B | Produto vai para a quarentena | Simples, mas as vendas desses SKUs viram órfãs e saem de toda a Gold: receita e volume reais deixariam de aparecer. |

### D2 · Clima no job e2e da CI (ENG-03)

A ingestão de clima chama a API Open-Meteo.

| Opção | Comportamento | Prós e contras |
|---|---|---|
| **A (recomendada)** | Fixture gravada: respostas reais da API para 2020–2025 versionadas em `tests/fixtures/open_meteo/`. A nova variável `CLIMA_FIXTURE_DIR` faz o `ingest_clima` ler os arquivos em vez de chamar a rede (≈10 linhas). | CI determinística e independente de serviço de terceiros. O caminho HTTP continua coberto por teste unitário com resposta simulada. |
| B | A CI chama a API real | Valida a integração de verdade, mas a CI pode falhar por rede ou limite de requisições, sem relação com o código. |

### D3 · Meta de cobertura (ENG-02)

- **A (recomendada):** relatório de cobertura sempre e **gate de 80%** apenas em `pipeline/common/` e nas funções de regra (`validate_vendas`, `deduplicate_clientes`, `parse_mixed_dates`, `normalize_ativo`, mapeamentos do financeiro). As funções `main()`, que fazem só entrada e saída, ficam cobertas pelo job e2e.
- B: gate global, que forçaria testes artificiais de I/O com mocks de S3.

### D4 · Estratégia de Git

- **Recomendado:** primeiro abrir e fazer o merge do PR `feat/sprint-1` → `main`. Depois, criar `feat/sprint-2` a partir da `main` atualizada. Mantém **um commit por card** (você commita) e um PR no final, que já vai rodar a CI nova.

### D5 · Python local

A imagem usa Python 3.12 e as versões fixadas (ex.: `pandas==2.1.4`) não têm wheels para o Python 3.13 instalado na sua máquina.
- **Recomendado:** adicionar `.python-version` com `3.12` (o `pyenv` já está instalado) e documentar `pip install -r requirements-dev.txt` num virtualenv. A CI usa 3.12.

---

## 2. Ordem de execução e dependências

```
Fase 1 – Infra        INF-04 (dbt sem root)
                         │
Fase 2 – Refatoração  ENG-01 (pipeline/common) ──► validação pelo e2e da Sprint 1 (sem mudança de comportamento)
                         │
Fase 3 – Testes       ENG-02 (pytest + cobertura) ──► DQ-09 (com testes desde o início)
                         │
Fase 4 – dbt          MOD-02 (testes mais rígidos + reconciliação)
                         │
Fase 5 – CI           ENG-03 (lint + unit + e2e com MinIO/Postgres + badge)
                         │
Fase 6 – Validação    PR com a CI verde + execução da DAG no ambiente real
```

- **INF-04 primeiro:** é pequeno e libera rodar o dbt fora do Docker durante o resto da sprint.
- **ENG-01 antes dos testes:** os testes passam a mirar os módulos finais, sem retrabalho.
- **ENG-03 por último:** a CI junta tudo e é validada pelo próprio PR da sprint.

---

## 3. Plano por card

### Fase 1: Infraestrutura

#### INF-04 · dbt sem root
- **`.env` / `.env.example`:** novas variáveis `HOST_UID`/`HOST_GID` (`id -u` / `id -g`).
- **`dags/etl_pipeline.py`:** `_dbt_common` com `user=f"{HOST_UID}:{HOST_GID}"` e `HOME=/tmp` no ambiente do container. O `dbt deps` precisa de um HOME gravável.
- **Limpeza única** das pastas que já pertencem ao root, documentada no README: `docker run --rm -v "$PWD/dbt:/w" alpine chown -R "$(id -u):$(id -g)" /w/logs /w/target /w/dbt_packages`.
- **Validação:** depois de uma execução da DAG, `find dbt -user root` não retorna nada, e um `dbt` local consegue escrever em `dbt/target`.
- **Resultado da execução:**
  - `HOST_UID`/`HOST_GID` são obrigatórias na DAG (falha cedo, como as demais) e `HOME=/tmp` no container.
  - Limpeza única feita: além das pastas previstas, `dbt/package-lock.yml` (versionado) e `dbt/.user.yml` também pertenciam ao root, porque o `dbt deps` reescreve esses arquivos.
  - Execução `sprint2_inf04`: 14/14 tasks `success`, **0 arquivos de root em `dbt/`**, e `dbt/target` foi gravado pelo usuário do host.

### Fase 2: Refatoração

#### ENG-01 · `pipeline/common/`
Estrutura proposta, **sem mudar comportamento**:

```
pipeline/common/
├── config.py      # absorve settings.py: require_env, s3_storage_options, postgres_url, ClimaConfig
├── storage.py     # s3 client, ensure_bucket, read_bronze(table) (glob + concat), write_silver(df, path, partition_cols),
│                  # write_quarantine(df, entity) — inclui a limpeza da execução anterior
├── validators.py  # PATTERN_ID_*, normalize_id, clean_text (absorve silver/text.py)
└── logging.py     # get_logger(name) com formato único (substitui 8 logging.basicConfig)
```

- As regras de negócio continuam em cada `clean_*.py` (`validate_vendas`, `deduplicate_clientes` etc.). O `common` só fica com o que é repetido.
- Remover `pipeline/settings.py`, `pipeline/silver/text.py` e o pacote vazio `pipeline/gold/`. Atualizar os imports da DAG.
- **Validação:** rodar o e2e da Sprint 1 (MinIO e Postgres isolados) e comparar com a referência: 3.482 vendas na fato, 18 na quarentena de vendas, 5 na de clientes e 65 testes dbt PASS. **Os números precisam ser idênticos.**
- **Resultado da execução:**
  - `pipeline/common/` com `config.py` (`S3Config`, `PostgresConfig`, `ClimaConfig` e `require_env`, este também usado pela DAG), `storage.py` (`get_s3_client`, `ensure_bucket`, `read_bronze`, `read_silver`, `write_silver`, `write_quarantine`), `validators.py` (`PATTERN_ID_*`, `normalize_id`, `clean_text`) e `logging.py` (`get_logger`). `settings.py`, `silver/text.py` e `pipeline/gold/` removidos.
  - A validação foi além das 4 contagens: um fingerprint (hash do conteúdo ordenado, sem `_ingested_at`) de **28 datasets** (Silver, as 2 quarentenas, as tabelas de `load_stage` e as tabelas e views de `gold`), o log do pipeline normalizado e a importação da DAG.
  - O baseline rodou 2× antes da refatoração, com resultados idênticos entre si (o harness é determinístico).
  - Depois da refatoração: **3.482 / 18 / 5 / 65 PASS**, os 28 fingerprints idênticos, a DAG com 14 tasks e `max_active_runs=1`, e 0 arquivos de root em `dbt/`.
  - Única diferença: o log perdeu uma linha INFO (`Bucket 'bronze' já existe`). Só a cópia de `ensure_bucket_exists` do clima registrava esse caso, e a versão unificada segue a do CSV.

### Fase 3: Testes e qualidade

#### ENG-02 · Testes unitários
- **Estrutura:** `tests/unit/` (funções puras) e `tests/integration/` (integridade da DAG), mais o `pyproject.toml` com a configuração de `pytest` e `coverage`.
- **Casos cobertos (mínimo):**

| Função | Casos |
|---|---|
| `parse_mixed_dates` | 4 formatos aceitos; data inexistente (`31/02/2024`), texto e nulo viram NaT; formatos não se confundem |
| `validate_vendas` | cada regra isolada; venda que viola várias regras (motivo combinado); entrada vazia; IDs minúsculos e com espaços |
| `deduplicate_clientes` | dedup por id mais recente; CNPJ duplicado mantém o menor id; resultado determinístico em ordens embaralhadas; id inválido, CNPJ e nome vazios |
| `repair_double_encoding` | corrige mojibake real; não altera texto legítimo com acento; conversão inválida devolve o original |
| `normalize_ativo`, mapas do financeiro | variações (`S`, `Sim`, `1`, `devolução`, `BONIF.` …) e valor desconhecido |
| `clean_text` / `normalize_id` | nulo continua nulo (nunca `"nan"`); vazio vira nulo; maiúsculas |
| `ClimaConfig` | coordenadas padrão; período invertido gera erro |
| `ingest_clima.fetch_weather_data_by_year` | resposta HTTP simulada (`unittest.mock`): sucesso, 404 e corpo sem `daily` |

- **Integridade da DAG** (`tests/integration/test_dag.py`, marcado como `airflow`): a DAG carrega sem erros de importação, tem 14 tasks, `max_active_runs == 1` e as dependências esperadas. Roda no job e2e, dentro da imagem do Airflow. No job unitário é pulado, porque o Airflow não é instalado lá.
- **Validação:** `pytest` verde localmente (Python 3.12) e cobertura ≥ 80% nos módulos do gate (D3).
- **Resultado da execução:**
  - `.python-version` (3.12), virtualenv com `requirements-dev.txt` (mais `requests`, `pytest-cov` e `coverage` fixados) e `pyproject.toml` com a configuração do pytest e da cobertura.
  - Para testar os mapeamentos do financeiro isoladamente, eles saíram do `main()` e viraram `MAP_TIPO_LANCAMENTO`/`MAP_STATUS`, `normalize_tipo_lancamento`, `normalize_status` e `parse_competencia`. O e2e rodou de novo depois disso: os 28 fingerprints continuam idênticos ao baseline, assim como o resumo do dbt e o log.
  - **160 testes unitários passando**, 1 pulado (o teste da DAG, sem Airflow no virtualenv), em ~2 s. Os casos da tabela acima foram cobertos, além de `fetch_all_weather_data` (falha parcial e total), `require_env`, `S3Config`/`PostgresConfig` e `storage` (com filesystem simulado).
  - **Gate da D3:** 100% nos módulos do gate (`pipeline/common/*`, `clean_clientes`, `clean_financeiro`, `clean_vendas`; `main()` excluído). Configurado com `fail_under = 80` no `pyproject.toml`. O teste negativo, rodando só um arquivo de teste, reprova com exit 1. O `clean_produtos` entra no gate no DQ-09, quando ganhar funções de regra.
  - Integridade da DAG na imagem do Airflow: 4/4 testes passando (importação sem erros nem ciclos, 14 tasks com as dependências esperadas, `max_active_runs == 1`, dbt com `HOST_UID:HOST_GID` e `HOME=/tmp`).

#### DQ-09 · Custo unitário *(depende da D1)*
- **`clean_produtos.py`:** `custo_unitario <= 0` ou nulo vira `NULL` com `custo_disponivel = false`, e o log lista os produtos. `preco_tabela <= 0` segue a mesma regra, com a flag `preco_disponivel`. Testes unitários escritos junto.
- **dbt:**
  - `stg_produtos`/`dim_produto` expõem `custo_disponivel`. A fato fica com `custo_total` e `margem_bruta` nulos quando não há custo.
  - Teste em `dim_produto`: `custo_unitario > 0` quando `custo_disponivel`.
- **`vw_q4`:** margem calculada só para produtos com custo, e os demais aparecem com margem nula. O Streamlit mostra "custo indisponível" na tabela e tira esses produtos do gráfico de margem.
- **Validação:** P012 e P019 sem margem de 100% na Q4; receita e volume desses SKUs mantidos nas Q1–Q3.
- **Resultado da execução:**
  - Silver: `flag_valores_indisponiveis` em `clean_produtos.py` (custo/preço `<= 0`, nulo ou não numérico vira NULL, com `custo_disponivel`/`preco_disponivel`). O log lista `['P012', 'P019']`. 13 testes unitários novos (173 no total), e o `clean_produtos` entrou no gate de cobertura (100%).
  - dbt: as flags foram expostas em `stg_produtos`/`dim_produto`; 4 testes novos (`not_null` das flags e 2 `expression_is_true` de coerência flag × valor), **69 PASS**. A `vw_q4` ganhou `custo_disponivel`, com custo e margem NULL quando a flag é falsa.
  - e2e isolado comparado ao baseline: mudaram só os 6 datasets de produtos/fato/Q4. Os outros 22, **incluindo `vw_q1`, `vw_q2` e `vw_q3`**, ficaram idênticos. Comparando com o HEAD anterior (worktree), a `fato_vendas` sem `custo_total`/`margem_bruta` é idêntica nas 3.482 linhas, e as 3.228 vendas dos demais SKUs são idênticas por inteiro. Só as 254 vendas de P012/P019 mudaram (custo e margem passaram a NULL).
  - Q4: nenhum produto com margem ≥ 100% (máxima de 83,97%, Crystal 500ml). P012/P019 aparecem com receita (R$ 720,64 e R$ 2.258,82 nas vendas pagas) e margem NULL.
  - Ambiente real: DAG `dq09_validacao` 14/14 `success`. A fato incremental ficou, como esperado, com `custo_total = 0.00` nas vendas antigas desses SKUs até o `dbt run --full-refresh --select fato_vendas+`. Sem o `+`, o full-refresh derruba em cascata as views `vw_q*`. O README foi corrigido nos dois pontos que mostravam o comando sem `+`. Depois do full-refresh: 3.482 linhas, receita total igual (R$ 597.343,23), 69 PASS e 0 arquivos de root em `dbt/`.
  - Streamlit: a tabela marca "custo indisponível" (coluna Observação) e ganhou um aviso visível abaixo dela, porque as linhas ficavam fora da área visível da tabela. O gráfico de margem só mostra produtos com custo. A captura `dashboard_q3_q4.png` foi atualizada.

### Fase 4: dbt

#### MOD-02 · Testes dbt mais rígidos
- `relationships` de `sk_cliente`/`sk_produto` na fato passam de `warn` para `error`. Os `not_null` dessas chaves também.
- `unique` em `stg_financeiro.id_venda` (hoje há no máximo 1 lançamento por venda; o teste protege a fato de duplicar linhas).
- Teste singular `assert_fato_reconcilia_com_staging.sql`: contagem e soma de `receita_liquida` na fato iguais às de `stg_vendas`. Ele teria pegado, na hora, a duplicação da Sprint 1.
- `accepted_values` com os domínios atuais: `dim_cliente.canal` (5 canais + `Não informado`), `dim_cliente.uf`, `dim_cliente.regiao` e `dim_produto.familia`. A lista exata sai dos dados na implementação.
- **Validação:** `dbt build` verde. Um teste negativo, introduzindo de propósito uma duplicata numa base de teste, precisa fazer o teste de reconciliação falhar.
- **Resultado da execução:**
  - `relationships` e `not_null` de `sk_cliente`/`sk_produto` na fato agora são `error` (sem órfãos na fato: 0 de 3.482). `unique` em `stg_financeiro.id_venda` (2.454 linhas, todas distintas).
  - `tests/assert_fato_reconcilia_com_staging.sql` compara contagem e soma de `receita_liquida` da fato com `stg_vendas` (arredondada em 2 casas).
  - `accepted_values` com os domínios reais: `canal` (5 + `Não informado`), `uf` (`MG`), `regiao` (4) e `familia` (7).
  - `dbt test`: **75 PASS** (eram 69), 0 WARN, 0 ERROR.
  - Teste negativo: com uma linha duplicada inserida na fato do ambiente de dev, o teste de reconciliação falhou (`FAIL 1`). A linha foi removida em seguida (fato de volta a 3.482) e o teste voltou a passar.
  - Os `accepted_values` são listas fechadas: uma UF, região, canal ou família nova na origem faz o build falhar de propósito, até a lista ser atualizada.

### Fase 5: CI

#### ENG-03 · GitHub Actions *(depende da D2)*
`.github/workflows/ci.yml`, disparado em `push` e `pull_request`, com dois jobs:

| Job | O que faz | Tempo estimado |
|---|---|---|
| `lint-unit` | Python 3.12 · `ruff check` + `ruff format --check` · `pytest tests/unit` com cobertura e gate (D3) | ~1–2 min |
| `e2e` | build da imagem do Airflow · services `bitnamilegacy/minio` e `postgres:15` · pipeline Python sobre `data/raw` (clima via fixture, D2) · `dbt deps/seed/build` com a imagem fixada · teste de integridade da DAG · consultas de verificação (fato = 3.482, quarentenas 18/5) | ~5–8 min |

- **Ruff:** configuração no `pyproject.toml`, com regras `E`, `F`, `I`, `B`, `UP` e `SIM` e `line-length = 110`. Hoje são 56 apontamentos com a configuração global da máquina; com a do projeto, eles serão corrigidos ou justificados no mesmo card.
- O **script e2e** da Sprint 1, que hoje está fora do repositório, vira `scripts/e2e.sh`. Ele é usado pela CI e pode ser rodado localmente.
- **Badge** de status da CI no topo do README.
- **Validação:** o PR da Sprint 2 abre com os dois jobs verdes. Um commit de teste com erro proposital de lint e um teste quebrado tem que deixar a CI vermelha.
- **Resultado da execução:**
  - **Clima (D2-A):** `tests/fixtures/open_meteo/{2020..2025}.json` são respostas reais da API (2.192 dias, sem nulos). `ClimaConfig.fixture_dir` lê `CLIMA_FIXTURE_DIR`; com ela, `fetch_weather_data_by_year` lê o arquivo do ano e não chama a rede. Ano sem fixture vira `ValueError` e entra no mesmo tratamento de falha parcial. 5 testes novos (178 no total) e o gate segue em 100%.
  - **Ruff:** `ruff==0.8.6` fixado no `requirements-dev.txt`; configuração no `pyproject.toml` (`E,F,I,B,UP,SIM`, `line-length = 110`, `dbt/`, `notebooks/` e `streamlit/` excluídos). Com a configuração do projeto, o `ruff check` apontou 28 itens (o plano estimava 56 com a configuração global) e o `ruff format --check` pediu mudanças em 21 arquivos. Corrigidos 3 de comportamento (`B904`, `F841`, `E701`), os 3 `SIM117` dos testes e o restante por `--fix` e `ruff format`. A formatação automática tocou 20 arquivos e deve ir num **commit separado** para não poluir os diffs de regra.
  - **`scripts/e2e.sh`:** o script da Sprint 1 não estava no repositório, então foi reescrito. Sobe MinIO e Postgres descartáveis (portas 19000/15432, `E2E_START_SERVICES=0` na CI), não lê o `.env`, roda Bronze, Silver, `load_stage`, `dbt deps/seed/build` (imagem fixada, `--network host`), `scripts/e2e_verify.py` e o teste da DAG na imagem do Airflow. Para isso o `dbt/profiles.yml` passou a ler `POSTGRES_HOST`/`POSTGRES_PORT` (com os padrões antigos). O container do dbt roda com o UID do host.
  - **Execução local do e2e:** `dbt build` com **91 PASS** (2 seeds, modelos e testes), fato = **3.482**, quarentenas **18/5**, receita total **R$ 597.343,23**, Q4 sem margem para produtos sem custo, DAG **4/4**.
  - **Achado no próprio e2e:** o teste da DAG era *pulado* (não falhava) quando o Airflow não era importável, e o job passaria verde sem testar nada. Com `REQUIRE_AIRFLOW=1` (definido pelo `e2e.sh`) a ausência do Airflow agora é erro.
  - **`.github/workflows/ci.yml`:** jobs `lint-unit` (`ruff check`, `ruff format --check`, `pytest tests/unit --cov` com o gate, `coverage.xml` como artefato) e `e2e` (MinIO e Postgres como *service containers*), com `concurrency` e `timeout-minutes`. Badge no topo do README, seções "Teste ponta a ponta" e "CI" em "Como rodar os testes".
  - **Teste negativo local:** com um import não usado, um arquivo mal formatado e um teste quebrado, `ruff check`, `ruff format --check` e `pytest` saem com código 1; revertido, os três voltam a passar.
  - **Pendente:** a execução no GitHub Actions só acontece quando o PR for aberto (Fase 6). Lá é preciso confirmar o `bitnamilegacy/minio` como *service container* (risco previsto na seção 6) e repetir o commit de teste com erro proposital.

---

## 4. Variáveis novas no `.env`

```dotenv
# UID/GID do host para o container dbt (INF-04)
HOST_UID=1000        # id -u
HOST_GID=1000        # id -g

# Somente CI/testes: lê respostas gravadas da Open-Meteo em vez de chamar a API (D2)
# CLIMA_FIXTURE_DIR=tests/fixtures/open_meteo
```

---

## 5. Validação final da sprint

1. CI verde no PR (`lint-unit` + `e2e`).
2. Ambiente real recriado: DAG com 14/14 tasks verdes, `dbt test` 100% PASS (com os novos testes), sem arquivos de root em `dbt/`.
3. Q4 sem margem de 100% para P012/P019 e números da Q1–Q3 inalterados em relação à Sprint 1.
4. README com o badge, a seção "Como rodar os testes" e as capturas atualizadas, se a Q4 mudar.

**Definição de pronto de cada card:** critérios do `BACKLOG.md` marcados, testes cobrindo a mudança, e2e com os números de referência, commit próprio com o ID do card.

---

## 6. Riscos e mitigação

| Risco | Mitigação |
|---|---|
| A refatoração do ENG-01 muda comportamento sem querer | Comparação obrigatória com os números de referência da Sprint 1, pelo e2e, antes do commit |
| Imagem `bitnamilegacy/minio` indisponível no runner da CI | Ela é pública no Docker Hub; se falhar, cache da imagem ou alternativa S3-compatível só na CI |
| Job e2e lento ou instável | Fixture de clima (D2), imagens com tags fixas, `timeout-minutes` no job |
| Ruff apontar muitos ajustes de estilo | Correções automáticas (`--fix`) num commit separado dentro do ENG-03, para não poluir os diffs de regra |
| `user=` no DockerOperator quebrar o `dbt deps` (HOME não gravável) | `HOME=/tmp` no ambiente do container, validado no ambiente real |

---

## 7. Fora do escopo desta sprint

- Idempotência por `logical_date`, escrita atômica, `COPY` e incremental com merge (Sprint 3). Os testes desta sprint são a base para fazê-los com segurança.
- Quarentena genérica e tabela de auditoria (ARQ-05, Sprint 3).
- Observabilidade e novos modelos (Sprint 4).

---

## Aprovação

- [x] D1: produtos com custo zero (opção: A)
- [x] D2: clima no job e2e (opção: A)
- [x] D3: meta de cobertura (opção: A)
- [x] D4: estratégia de Git (PR #1 da Sprint 1 mergeado; branch `feat/sprint-2` criada)
- [x] D5: Python local 3.12
- [x] Plano aprovado
