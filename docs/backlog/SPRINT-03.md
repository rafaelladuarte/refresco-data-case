# Sprint 3: Pipeline robusto

> **Status:** 🟢 Aprovado em 25/11/2026 (todas as decisões conforme as recomendações) · em execução
> **Escopo:** os 6 cards da coluna "Sprint 3" do [`BACKLOG.md`](../../BACKLOG.md): ARQ-01, ARQ-02, ARQ-03, ARQ-05, MOD-01 e ARQ-04
> **Objetivo:** atacar as causas raiz vistas na validação da Sprint 1: execução idempotente, escrita atômica, carga tipada, incremental correto e auditoria de qualidade.

**Por que agora.** A Sprint 2 entregou o que permite mexer no núcleo com segurança: 178 testes unitários, o e2e com números de referência (`scripts/e2e.sh`) e a CI. A Sprint 3 muda justamente escrita, carga e incremental, onde estavam os bugs mais caros (corrupção por execuções simultâneas, fato que perde vendas retroativas).

**Os problemas de hoje, em uma linha cada:**

| Problema | Onde | Card |
|---|---|---|
| Cada execução grava uma partição Bronze com `datetime.now()` e a Silver relê `bronze/**` inteira: custo cresce sem limite, sem backfill | `ingest_csv.py`, `read_bronze` | ARQ-01 |
| `fs.rm()` seguido de `to_parquet()`: falha no meio deixa a Silver vazia ou parcial | `write_silver`, `write_quarantine` | ARQ-02 |
| `DROP TABLE ... CASCADE` derruba as views `stg_*` a cada carga e os tipos ficam a cargo do pandas | `pipeline/load/staging.py` | ARQ-03 |
| `data_emissao > max(data_emissao)` perde vendas retroativas e mudanças de status financeiro | `fato_vendas.sql` | MOD-01 |
| Só `vendas` e `clientes` gravam quarentena; produtos só faz log; não há auditoria | `clean_*.py` | ARQ-05 |
| `receita_liquida` é calculada no pandas e `margem_bruta` no dbt | `clean_vendas.py`, `fato_vendas.sql` | ARQ-04 |

---

## 1. Antes de aprovar: decisões pendentes

### D1 · Partição Bronze e partições antigas (ARQ-01)

Hoje: `bronze/<tabela>/ingestion_date=2026-11-25T12:17:55/`. Passa a ser `ingestion_date=2026-11-25` (a `logical_date` da DAG, `{{ ds }}`), então reexecutar o dia sobrescreve a mesma partição.

| Opção | Comportamento | Prós e contras |
|---|---|---|
| **A (recomendada)** | Só partições no formato `YYYY-MM-DD` são lidas. As antigas (com horário) são ignoradas com um warning que lista os caminhos. Limpeza única documentada no README (`fs.rm` das partições antigas, ou `docker compose down -v` num ambiente de teste). | Sem código de migração. Nenhuma execução antiga pode "vencer" uma nova por engano. |
| B | Converter as partições antigas para a data delas | Preserva o histórico, mas é código descartável para dados que são só uma cópia dos CSVs de origem. |

### D2 · Como a Silver faz o merge (ARQ-01)

A fonte é um **snapshot completo** (`data/raw/*.csv`), não deltas diários. A Silver processa só as partições novas e faz merge com o estado atual por chave natural.

| Opção | Comportamento | Prós e contras |
|---|---|---|
| **A (recomendada)** | Lógica incremental, escrita física completa: lê o estado atual da Silver, lê as partições Bronze posteriores à marca d'água, une, deduplica por chave (precedência: `_logical_date`, depois `_ingested_at`) e regrava a tabela inteira de forma atômica (ARQ-02). | Simples e correto no tamanho atual (≈3,5 mil vendas). O custo de leitura da Bronze deixa de crescer. Backfill fora de ordem funciona, porque a precedência é pela data lógica e não pelo relógio. |
| B | Sobrescrever só as partições Silver afetadas (`ano`/`mes`) | Mais eficiente em volume grande, mas exige reconciliar mudanças de chave entre partições (uma venda que muda de mês). Complexidade sem ganho hoje. |

A marca d'água é derivada da própria Silver (`max(_logical_date)` por tabela), sem arquivo de estado separado.

### D3 · O que marca uma linha como "alterada" (ARQ-01 e MOD-01)

Como cada dia reingere o snapshot inteiro, **toda linha ganha um `_ingested_at` novo todo dia**, mesmo sem mudança. Um incremental filtrando por `ingested_at` reprocessaria tudo sempre.

| Opção | Comportamento | Prós e contras |
|---|---|---|
| **A (recomendada)** | A Silver ganha `_row_hash` (hash do conteúdo de negócio) e `_changed_at`. `_changed_at` só avança quando o hash muda (ou a linha é nova). O incremental do dbt filtra por `_changed_at`. | O incremental é de fato incremental e a mudança de status de uma venda antiga é detectada com precisão. |
| B | Filtrar por `_ingested_at` e confiar no `merge` do dbt | Menos código, mas o "incremental" passa a reprocessar a tabela inteira a cada dia. |

### D4 · Atomicidade no MinIO/S3 (ARQ-02)

S3 não tem `rename` atômico. Nenhuma opção entrega atomicidade perfeita; a escolha é onde fica o risco.

| Opção | Comportamento | Prós e contras |
|---|---|---|
| **A (recomendada)** | Escreve em `silver/_tmp/<run_id>/<tabela>/`, valida (relê e confere linhas e colunas) e só então troca: move a versão atual para `_prev/`, move a nova para o destino e apaga `_prev/`. Falha antes da troca não toca na versão atual; falha durante a troca é revertida a partir de `_prev/`. | Nenhum leitor muda. A janela de risco cai de "toda a escrita" para alguns segundos da troca, e a recuperação é automática. Com `max_active_runs=1` e o `load_to_staging` só depois da Silver, ninguém lê nesse intervalo. |
| B | Versionar o prefixo (`_v=<run_id>/`) com um arquivo ponteiro `_current` | Troca realmente atômica (um PUT), mas todo leitor (clean_*, `load_to_staging`, notebooks) precisa resolver o ponteiro. |

### D5 · Como as views `stg_*` sobrevivem (ARQ-03)

Com `TRUNCATE` + `COPY` as tabelas de `load_stage` nunca são derrubadas, então as views continuam válidas.

| Opção | Comportamento | Prós e contras |
|---|---|---|
| **A (recomendada)** | A task `dbt_run_staging` deixa de ser uma etapa própria e as views passam a ser recriadas junto com a Gold (`--select staging marts.gold`). A DAG vai de 14 para 13 tasks. | As views deixam de ser pré-requisito a cada execução, mas continuam sendo recriadas se um modelo mudar. |
| B | Manter a task como está | Nada muda na DAG, mas o critério "deixa de ser obrigatório" fica só no papel. |

### D6 · Onde mora a auditoria (ARQ-05)

| Opção | Comportamento | Prós e contras |
|---|---|---|
| **A (recomendada)** | `audit.dq_results` no Postgres da Gold (schema `audit`), gravada por cada task Silver, com o `run_id` do Airflow. A tabela é criada pela DDL versionada do ARQ-03. | Consultável por SQL e pelo Streamlit na Sprint 4. A Silver passa a depender do Postgres. |
| B | Parquet em `silver/_audit/` | Sem dependência nova, mas não é "tabela" e é difícil de consultar. |

A quarentena de **todas** as entidades vai para `silver/_quarentena/<entidade>/<data>/`. Reexecutar o mesmo dia sobrescreve só a pasta daquela data; as demais datas ficam como histórico.

### D7 · ARQ-04: quanto mover para o dbt

- **Recomendado:** `receita_liquida` (`qtd_caixas * preco_unitario * (1 - desconto_pct)`) passa a ser calculada em `stg_vendas`. A Silver só limpa, tipa e valida; a regra de desconto continua validada na Silver (`0 <= desconto < 1`). A fórmula é documentada no `schema.yml`. Os números não devem mudar (arredondamento em 2 casas, igual ao de hoje), e o teste de reconciliação do MOD-02 e a receita total (R$ 597.343,23) provam isso.
- Alternativa: deixar o ARQ-04 para a Sprint 4. Ele tem prioridade 🟢 baixa e só toca o schema da Silver, mas está no escopo da sprint e vem por último, sem bloquear os demais.

### D8 · Git e validação de refatoração

- **Estrutura:** `feat/sprint-3` criada a partir da `main` atualizada (já criada). Um commit por card e um PR no final, com a CI rodando.
- **Recomendado:** antes de começar, trazer para o repositório a verificação por *fingerprint* usada na Sprint 2 (hash do conteúdo ordenado de Silver, quarentenas, `load_stage` e Gold). O script da Sprint 1 e o da Sprint 2 não ficaram no repositório. Sem ele, ARQ-02 e ARQ-03 (que **não podem mudar dados**) só são conferidos pelas 5 contagens do e2e.

---

## 2. Ordem de execução e dependências

```
Fase 0 – Rede de segurança  fingerprint.py no repo + e2e com cenário de 2 dias (baseline na main)
                               │
Fase 1 – Escrita            ARQ-02 (escrita atômica) ──► fingerprints idênticos
                               │
Fase 2 – Ingestão           ARQ-01 (logical_date, partições, merge, _changed_at)
                               │
Fase 3 – Qualidade          ARQ-05 (quarentena por data + audit.dq_results)
                               │
Fase 4 – Carga              ARQ-03 (DDL + TRUNCATE/COPY, views sobrevivem) ──► fingerprints idênticos
                               │
Fase 5 – Gold               MOD-01 (incremental com merge, lookback) ──► ARQ-04 (métricas no dbt)
                               │
Fase 6 – Validação          PR com a CI verde + DAG no ambiente real + backfill
```

- **ARQ-02 primeiro:** é a base de escrita de todos os demais e não deve mudar nenhum dado, então os fingerprints o comprovam.
- **ARQ-01 antes de ARQ-05:** a quarentena por data depende da `logical_date`.
- **ARQ-03 antes de MOD-01:** os tipos e a estabilidade das views dão base ao incremental.
- **MOD-01 depois de ARQ-01:** precisa do `_changed_at` (D3).
- **ARQ-04 por último:** muda o schema da Silver e deve ser conferido contra a Gold já estável.

---

## 3. Plano por card

### Fase 0: rede de segurança

- **`scripts/fingerprint.py`:** calcula um hash do conteúdo ordenado (sem `_ingested_at`, `_changed_at`, `_logical_date`) de cada dataset: Silver, as quarentenas, as tabelas de `load_stage` e as tabelas e views de `gold`. Gera um JSON para comparar entre commits.
- **`scripts/e2e.sh` ampliado** com um cenário de **dois dias**: o dia 1 usa `data/raw`; o dia 2 usa uma cópia em diretório temporário (via `AIRFLOW_HOME`) com mudanças conhecidas: o status financeiro de uma venda antiga alterado, uma venda retroativa nova e um cliente alterado. O script confere que só essas linhas mudaram.
- **Validação:** baseline gerado na `main`, antes de qualquer card.
- **Resultado da execução:**
  - `scripts/fingerprint.py` (`gerar` e `comparar`): hash do conteúdo ordenado de **28 datasets** (Silver, as 2 quarentenas, `load_stage` e `gold`, tabelas e views), ignorando linhas e colunas na ordem e as colunas de controle (`_ingested_at`, `_changed_at`, `_logical_date`, `_row_hash` e similares). 10 testes unitários (188 no total).
  - **Baseline versionado** em `tests/baseline/fingerprint.json`, gerado com o código da `main`. O `e2e.sh` compara o dia 1 com ele em toda execução (local e CI). Só muda com `E2E_UPDATE_BASELINE=1`, justificado no commit do card que alterar dados de propósito (ex.: ARQ-04, que tira `receita_liquida` da Silver).
  - **Cenário de dois dias** no `e2e.sh`: `e2e_mutate.py` copia `data/raw` e aplica 3 mudanças escolhidas na Gold do dia 1 (status financeiro de uma venda antiga e paga, uma venda retroativa `V900001` e o nome de um cliente). `e2e_verify_day2.py` confere que só elas aparecem e que as quarentenas (18/5) não mudam.
  - **O bug do MOD-01 já aparece:** no dia 2 o nome do cliente propaga, mas a venda retroativa e a mudança de status **não chegam à fato**, e o teste de reconciliação (MOD-02) falha. Essas 3 checagens saem como `[PENDENTE]` (não reprovam o e2e) e a falha tolerada do dbt é só a do `assert_fato_reconcilia_com_staging`; qualquer outra reprova. Com `E2E_ENFORCE_PENDING=1` elas passam a reprovar. A lista de pendentes e a tolerância são removidas no commit do MOD-01.

### Fase 1: ARQ-02 · Escrita atômica

- **`pipeline/common/storage.py`:** `write_silver` e `write_quarantine` passam a usar o protocolo da D4-A (`_tmp/<run_id>/`, validação por releitura, troca com `_prev/`, reversão em caso de falha). Limpeza de `_tmp/` e `_prev/` órfãos no início de cada escrita.
- **Testes (filesystem simulado, como os de `storage` de hoje):** falha na escrita do tmp preserva a versão anterior; falha na validação preserva; falha durante a troca restaura a partir de `_prev/`; sucesso não deixa `_tmp/` nem `_prev/`.
- **Validação:** 178 testes verdes e fingerprints idênticos ao baseline. Teste manual: derrubar o MinIO no meio de uma escrita não deve deixar a Silver vazia.
- **Resultado da execução:**
  - `write_silver` e `write_quarantine` usam `_write_atomically` (D4-A): grava em `<bucket>/_tmp/<run_id>/<dataset>`, valida relendo (linhas e colunas), move a versão atual para `<bucket>/_prev/<dataset>`, publica a nova e apaga `_prev/`. Se a publicação falhar, a antiga é restaurada. Se o processo cair no meio da troca, a próxima escrita do mesmo dataset restaura `_prev/` (ou o apaga, se a troca já tinha terminado) e remove `_tmp/` órfão **só do próprio dataset**, porque as tasks Silver irmãs gravam em paralelo.
  - A escrita passa a ir pelo próprio cliente `fs` (`filesystem=`), em vez de `storage_options`, o que permitiu testar o protocolo de verdade num filesystem em memória com caminhos no estilo S3.
  - **18 testes de escrita** (13 de Silver e 5 de quarentena, no lugar dos 5 baseados em mock; 201 no total): sucesso, primeira escrita, arquivo único, falha na gravação, gravação truncada barrada pela validação, falha ao publicar, queda no meio da troca (com e sem nova falha em seguida), `_prev/` sobrando, `_tmp/` órfão sem tocar nas tasks irmãs, datasets independentes e as situações da quarentena (gravação, substituição, vazia com e sem anterior, falha).
  - **`scripts/e2e_atomic_check.py`** roda no e2e contra o MinIO real, com falha injetada na gravação e na publicação, e confere que não sobra `_tmp/` nem `_prev/` na Silver.
  - **Falha intermitente do MinIO:** em uma das execuções do e2e, a primeira gravação falhou com `ClientDisconnected` no `PutObject`. Não consegui reproduzir (38 tentativas isoladas, com MinIO novo e com bucket recriado, mais duas execuções completas do e2e), então a causa continua desconhecida. Como a gravação no prefixo temporário é privada e descartável, ela agora é refeita até 3 vezes (`WRITE_ATTEMPTS`, 2 s entre tentativas), sempre limpando o prefixo antes de repetir. Falha persistente continua reprovando e preservando a versão atual. Se voltar a aparecer na CI, vale investigar.
  - **Mudança de comportamento:** um DataFrame **vazio** com `partition_cols` agora falha na validação (o pyarrow não grava arquivo nenhum) e preserva a versão anterior, em vez de deixar um diretório vazio.

### Fase 2: ARQ-01 · Ingestão idempotente por `logical_date`

- **DAG:** `ds` (`{{ ds }}`) passado por `op_kwargs` aos `main()` de Bronze e Silver (`main(logical_date=None)`, com padrão = data de hoje para execução manual).
- **Bronze (`ingest_csv.py`, `ingest_clima.py`):** partição `ingestion_date=<logical_date>` (D1-A) e nova coluna `_logical_date`. Reexecutar o dia sobrescreve a partição (usa a escrita atômica).
- **`read_bronze`:** lê só as partições `YYYY-MM-DD` posteriores à marca d'água da Silver (D2-A). Partições antigas, com horário, geram warning (D1-A).
- **Silver (`clean_*.py`):** merge por chave natural com precedência (`_logical_date`, `_ingested_at`). Nova função comum de merge em `pipeline/common/`, com testes unitários. `_row_hash` e `_changed_at` (D3-A).
- **Backfill:** `airflow dags backfill` sobre um período, na ordem e fora de ordem; o resultado final precisa ser igual. Documentado no README.
- **Testes:** idempotência (rodar duas vezes o mesmo dia = mesmo resultado), precedência por `logical_date` e não por relógio, `_changed_at` estável sem mudança de conteúdo, e merge com chave nova, alterada e removida da origem.
- **Validação:** o e2e de dois dias; no dia 1 os números de referência (3.482 / 18 / 5) se mantêm.
- **Resultado da execução:**
  - **Refinamento da D2 (marca d'água):** a Silver não usa `max(_logical_date)` como marca d'água. Cada execução processa **exatamente a partição da sua `ds`** e faz o merge com precedência por data lógica. Com marca d'água, o backfill de uma data antiga (anterior à marca) seria pulado; com a partição da `ds`, qualquer ordem funciona, e o custo de leitura da Bronze continua limitado a uma partição. Partição ausente para a data vira erro (`FileNotFoundError`), em vez de o task terminar "com sucesso" sem fazer nada.
  - **Data lógica:** `main(ds=None)` em Bronze e Silver. No Airflow o `PythonOperator` injeta `ds` em quem declara esse parâmetro (a DAG não mudou); fora dele, `python -m pipeline.silver.clean_vendas 2024-06-01`, e sem argumento vale "hoje". Teste da DAG confirma que as 7 tasks recebem `ds`.
  - **Bronze:** `ingestion_date=<ds>` e coluna `_logical_date`. Partições antigas (com horário) são ignoradas com um warning (D1-A); `scripts/cleanup_legacy_bronze.py` as lista (dry-run) e, com `--apply`, as apaga.
  - **`pipeline/common/merge.py`:** `merge_state` (precedência `_logical_date`, `_ingested_at`; `_changed_at` só avança se `_row_hash` mudar; linha que some da origem permanece), `row_hash` (nulos equivalentes, ordem de colunas irrelevante). Aceita uma Silver antiga sem colunas de controle (o primeiro merge as completa; as linhas existentes podem ter `_changed_at` renovado uma vez).
  - **Bug evitado:** o `concat` do pandas 2.1.4 quebra ao misturar `datetime64[ns]` com outras resoluções (que o Parquet ou um escalar `Timestamp` podem produzir). O merge normaliza as datas de controle para `ns`.
  - **Testes (243 no total):** 27 do merge (idempotência, backfill fora de ordem, ordem de execução irrelevante, chave composta, Silver antiga), data lógica, leitura da Bronze por data com aviso de partições antigas, `read_silver_state` (colunas de partição voltam ao tipo original), ingestão por partição (idempotente) e a limpeza das partições antigas.
  - **e2e:** o dia 1 roda com `ds=2024-06-01` e o dia 2 com `2024-06-02`. Depois do dia 2, o e2e reexecuta o dia 2 (idempotência) e o **dia 1 depois do dia 2** (backfill fora de ordem), e exige que a Silver fique idêntica por fingerprint. O dia 1 continua idêntico ao baseline (28 datasets).
  - **Fora desta execução:** o backfill com `airflow dags backfill` no ambiente real (Fase 6) e a unicidade de CNPJ entre datas (dentro da partição segue valendo; entre datas fica para o ARQ-05).

### Fase 3: ARQ-05 · Quarentena genérica e auditoria

- **`write_quarantine`** grava em `silver/_quarentena/<entidade>/<data>/` para **todas** as entidades. `clean_produtos` e `clean_financeiro` passam a ter regras de rejeição explícitas, com motivo, em vez de só log. A lista exata de regras sai da análise dos dados na implementação; se os dados atuais não rejeitarem nada, o caso é coberto por teste unitário.
- **`audit.dq_results`** (`run_id`, `entidade`, `lidas`, `aprovadas`, `rejeitadas`, `motivo`, `registrado_em`), criada pela DDL do ARQ-03. Cada task Silver grava uma linha por motivo de rejeição, mais uma linha-resumo. Reexecutar o mesmo `run_id` não duplica (upsert por `run_id`, `entidade`, `motivo`).
- **Testes:** contagem `lidas = aprovadas + rejeitadas` em cada entidade; motivo agregado; reexecução não duplica.
- **Validação:** após o e2e, `audit.dq_results` tem uma linha-resumo por entidade e as rejeitadas batem com as quarentenas (18 e 5).

### Fase 4: ARQ-03 · Carga com DDL e `COPY`

- **`pipeline/load/ddl/load_stage.sql`:** `CREATE TABLE IF NOT EXISTS` com tipos explícitos (`ano`/`mes` inteiros, `numeric`, `date`/`timestamp`) para as 5 tabelas, mais o schema `audit`.
- **`staging.py`:** aplica a DDL, e por tabela, numa transação: `TRUNCATE` + `COPY` (`psycopg2.copy_expert` a partir de um buffer CSV). Falha no meio faz rollback e preserva a carga anterior. Se as colunas da Silver divergirem da DDL, falha com mensagem clara (detecta desvio de schema cedo).
- **DAG (D5-A):** `dbt_run_staging` incorporada à `dbt_run_gold` (`--select staging marts.gold`); teste da DAG atualizado para 13 tasks.
- **Validação:** fingerprints de Silver e Gold idênticos ao baseline (a mudança de tipos não pode alterar valores). Uma segunda carga com as views `stg_*` existentes não as derruba.

### Fase 5: MOD-01 · Incremental da `fato_vendas`

- **`fato_vendas.sql`:** `incremental_strategy='merge'` com `unique_key='id_venda'`. O filtro passa a ser por mudança, não por `data_emissao`: uma venda entra se `greatest(v._changed_at, f._changed_at) > max(atualizado_em)` da fato, **ou** se `data_emissao >= current_date - var('lookback_days', 30)`. O lookback cobre mudanças em dimensões (por exemplo, o custo de um produto), que o `_changed_at` da venda não enxerga.
- **`stg_financeiro`** passa a expor `ingested_at` e `changed_at` (hoje o `ingested_at` não é selecionado), e a fato ganha `atualizado_em`.
- **Teste negativo (e2e de dois dias):** alterar o status financeiro de uma venda antiga e confirmar a propagação para a fato **sem** `--full-refresh`. Esse caso falha na implementação atual.
- **Documentar:** mudanças em dimensões fora da janela de lookback exigem `dbt run --full-refresh --select fato_vendas+` (comando já descrito no README).

### Fase 5: ARQ-04 · Regras de negócio no dbt

- **`clean_vendas.py`** deixa de calcular `receita_liquida`; `stg_vendas` calcula `round(qtd_caixas * preco_unitario * (1 - desconto_pct), 2)` (D7).
- **`schema.yml`** documenta as fórmulas de `receita_liquida`, `custo_total` e `margem_bruta`.
- **Validação:** receita total R$ 597.343,23 e o teste de reconciliação continuam passando; `vw_q1..vw_q4` inalteradas contra o baseline.

---

## 4. Variáveis e mudanças de configuração

Nenhuma variável nova no `.env`. Mudanças:

- Task `dbt_run_staging` removida da DAG (13 tasks) e `--select staging marts.gold` na `dbt_run_gold`.
- Novo schema `audit` no Postgres da Gold, criado pela DDL do ARQ-03.
- Nova variável do dbt `lookback_days` (padrão 30), documentada no README.

---

## 5. Validação final da sprint

1. CI verde no PR, incluindo o e2e de dois dias.
2. Reexecutar o mesmo dia 2× na DAG real: mesma Silver, mesma Gold (idempotência).
3. Backfill de um período (3 dias) em ordem e fora de ordem, com resultados iguais.
4. Status de uma venda antiga alterado na origem aparece na Gold sem `--full-refresh`.
5. Falha injetada no meio de uma escrita Silver deixa a versão anterior intacta.
6. Os números de referência da Sprint 2 (3.482 / 18 / 5 / 75 testes dbt / R$ 597.343,23) inalterados.
7. README atualizado (backfill, lookback, limpeza das partições antigas, `audit.dq_results`).

**Definição de pronto de cada card:** critérios do `BACKLOG.md` marcados, testes cobrindo a mudança, e2e com os números de referência e fingerprints conferidos, commit próprio com o ID do card.

---

## 6. Riscos e mitigação

| Risco | Mitigação |
|---|---|
| A troca na escrita atômica ainda tem uma janela de segundos no S3 | Recuperação automática a partir de `_prev/` e `max_active_runs=1`; documentado como limitação (a D4-B elimina a janela, se for exigido) |
| O merge da Silver muda dados sem querer | Fingerprints contra o baseline e e2e de dois dias; o cenário do dia 1 precisa dar os mesmos números |
| `COPY` com tipos explícitos rejeita valores que o pandas aceitava | A DDL é validada contra a Silver real no e2e; divergência de schema falha cedo com mensagem clara |
| O `_row_hash` muda por mudança de formato (não de conteúdo) e reprocessa tudo | O hash cobre só colunas de negócio, em ordem fixa, com tipos normalizados; teste de estabilidade |
| Incremental com `merge` deixa de refletir mudança de dimensão | Lookback configurável e comando de `--full-refresh` documentado |
| Partições Bronze antigas atrapalham o primeiro run | Ignoradas com warning (D1-A) e limpeza única documentada |
| Sprint grande (3 cards `M` e 1 `G`) | A ordem das fases deixa cada card entregável sozinho; ARQ-04 pode escorregar para a Sprint 4 sem afetar os demais |

---

## 7. Fora do escopo desta sprint

- Grão financeiro, `dim_representante`, SCD2 e fato de lançamentos (MOD-03, MOD-04 e MOD-05, Sprint 4).
- Observabilidade: o dashboard de auditoria, alertas e métricas de execução (Sprint 4). Esta sprint só **grava** `audit.dq_results`.
- Atomicidade real do S3 por ponteiro (D4-B) e escrita Silver por partição (D2-B).

---

## Aprovação

- [x] D1: partição Bronze e partições antigas (recomendado: A)
- [x] D2: merge da Silver (recomendado: A)
- [x] D3: marca de linha alterada (recomendado: A)
- [x] D4: atomicidade no S3 (recomendado: A)
- [x] D5: views `stg_*` (recomendado: A)
- [x] D6: auditoria (recomendado: A)
- [x] D7: escopo do ARQ-04 (recomendado: incluir)
- [x] D8: Git e fingerprint no repositório (recomendado)
- [x] Plano aprovado
