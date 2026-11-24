# Sprint 1: plano de implementação

> **Status:** 🟢 Aprovado em 24/11/2026 (todas as decisões conforme as recomendações) · em execução
> **Escopo:** os 13 cards da coluna "A Fazer — Sprint 1" do [`BACKLOG.md`](../../BACKLOG.md)
> **Objetivo:** corrigir o que altera os números do dashboard, tornar o build reproduzível e alinhar a documentação com o código.

---

## 1. Antes de aprovar: decisões pendentes

Estes pontos mudam regras de negócio ou números exibidos e precisam de aprovação explícita. Cada um traz uma recomendação.

### D1 · Clientes com CNPJ duplicado (DQ-08): **achado novo, prioridade elevada para 🔴**

Ao detalhar o DQ-08, encontrei um bug mais grave do que o descrito no backlog. `clientes.csv` tem 5 empresas cadastradas duas vezes, com IDs diferentes:

| Cadastro original | Recadastro | Vendas no original | Vendas no recadastro |
|---|---|---|---|
| C0001 Distribuidora União | C0200 | sim | 0 |
| C0002 Atacadão do Zé | C0201 (nome com encoding corrompido) | sim | 0 |
| C0003 Empório Santa Fé | C0202 | sim | 0 |
| C0004 Padaria da Praça | C0203 | sim | 0 |
| C0005 Mercado do João | C0204 | sim | 0 |

O `drop_duplicates(subset=["cnpj"], keep="last")` mantém justamente os recadastros **sem vendas** e descarta os originais. Com isso, **147 vendas legítimas (4,2%) vão para a quarentena** como "id_cliente sem cadastro". Os "órfãos C0004/C0005" citados em `docs/diagnostico_orfaos_silver.md` não são um problema da origem: **são criados pelo próprio pipeline**.

| Opção | Comportamento | Prós e contras |
|---|---|---|
| **A (recomendada)** | Chave = `id_cliente`. Para cada CNPJ duplicado, mantém o **menor `id_cliente`** (cadastro mais antigo); os demais vão para `silver/_quarentena/clientes/` com motivo `"CNPJ duplicado de C0001"` | 1 empresa = 1 linha na dimensão; as 147 vendas são recuperadas; o recadastro fica auditável |
| B | Chave = `id_cliente`, mantém todos os cadastros e adiciona a flag `cnpj_duplicado = true` | Mais simples, mas a dimensão fica com empresas duplicadas |

### D2 · Regra de negócio por `tipo_lancamento` (DQ-06)

Cada venda tem no máximo 1 lançamento financeiro. Portanto, o tipo **classifica a própria venda**: uma venda com lançamento `DEVOLUCAO` foi devolvida. Distribuição atual, considerando só as vendas com lançamento:

| tipo \ status | PAGO | PENDENTE | CANCELADO | NULL |
|---|---|---|---|---|
| VENDA | 224 | 193 | 193 | 0 |
| BONIFICACAO | 214 | 227 | 190 | 0 |
| DEVOLUCAO | 219 | 199 | 178 | 0 |
| AJUSTE | 195 | 213 | 202 | 10 |
| OUTRO | 2 | 0 | 5 | 0 |

Hoje as views usam todas as 854 vendas `PAGO`, inclusive as devolvidas.

**Proposta (recomendada):** um seed `dbt/seeds/regra_tipo_lancamento.csv` define o comportamento de cada tipo, e as views passam a usar as flags em vez de listar os tipos em SQL:

| tipo_lancamento | entra_receita | entra_volume | Justificativa |
|---|---|---|---|
| VENDA | ✅ | ✅ | Venda faturada |
| BONIFICACAO | ❌ | ✅ | Produto entregue sem cobrança: conta no volume e no custo, mas não na receita |
| DEVOLUCAO | ❌ | ❌ | A venda foi desfeita |
| AJUSTE | ❌ | ❌ | Lançamento contábil, não uma operação comercial |
| OUTRO | ❌ | ❌ | Sem semântica definida |

O filtro `status_financeiro = 'PAGO'` continua valendo.

> ⚠️ **Impacto:** a receita das views passa a ser calculada sobre ~224 vendas (antes: 854) e o volume sobre ~438. Os números do dashboard **vão cair bastante**. Isso é esperado: a distribuição dos dados sintéticos é uniforme entre os tipos. O README vai explicar a regra.

### D3 · Valores das metas (DQ-05)

O dataset **não tem metas**, e qualquer meta derivada do próprio realizado volta a ser circular (no agregado anual, `Σ realizado / Σ (realizado × fator) = 1/fator`). Opções:

- **A (recomendada):** valores redondos e fixos por canal × mês, escolhidos uma única vez e congelados no seed. O arquivo e o README dizem claramente que são **valores fictícios para demonstração**.
- B: você fornece os valores.

### D4 · Estratégia de Git

- **Recomendado:** commitar `BACKLOG.md` e este plano na `docs/backlog`, abrir a branch `feat/sprint-1` a partir dela e fazer **um commit por card** (Conventional Commits, mensagem com o ID do card, ex.: `fix(silver): deduplica clientes por id_cliente [DQ-08]`). No final, um PR para a `main`.

### D5 · Recriação dos volumes Docker

O INF-02 muda credenciais do Postgres do Airflow e a Fernet key. O Postgres só lê as credenciais na **primeira** inicialização do volume, então a validação final exige `docker compose down -v`. Isso **apaga os dados locais** (Airflow, MinIO e Postgres Gold), que são 100% reproduzíveis rodando a DAG. Também será preciso atualizar o seu `.env` local com as variáveis novas (lista na seção 4).

---

## 2. Ordem de execução e dependências

A ordem prioriza um build reproduzível primeiro, depois as correções em sentido upstream → downstream (Bronze → Silver → dbt), e a documentação por último, para que ela reflita o estado final.

```
Fase 1 – Infra        INF-01 ──► INF-02 ──► INF-03
                                               │
Fase 2 – Bronze       DQ-01 (encoding) ──► DQ-02 (clima)
                         │
Fase 3 – Silver       DQ-03 ("nan") ──► DQ-08 (clientes) ──► DQ-04 (medidas vendas)
                                                                  │
Fase 4 – dbt          DQ-07 (dim_tempo) ──► DQ-06 (regra tipo) ──► DQ-05 (metas)
                                                                  │
Fase 5 – Docs         DOC-02 ──► DOC-01
                                                                  │
Fase 6 – Validação    execução ponta a ponta + checklist da seção 5
```

---

## 3. Plano por card

### Fase 1: Infraestrutura

#### INF-01 · Fixar versões de dependências e imagens
- **`Dockerfile`**
  - Usar `FROM apache/airflow:2.9.0-python3.12`, com tag explícita de Python, em vez da tag implícita.
  - Instalar com o constraints oficial: `pip install -r requirements.txt -c https://raw.githubusercontent.com/apache/airflow/constraints-2.9.0/constraints-3.12.txt`.
  - Trocar `pip install apache-airflow-providers-docker` sem versão por uma entrada no `requirements.txt`, também resolvida pelo constraints.
- **`requirements.txt`**
  - Remover `apache-airflow` e `pytest`.
  - Fixar `pandas`, `pyarrow`, `s3fs`, `requests`, `python-dotenv`, `sqlalchemy` e `psycopg2-binary` nas versões do constraints.
- **`requirements-dev.txt`** (novo): `pytest` e `ipykernel`, para os notebooks.
- **`Pipfile` / `Pipfile.lock`:** remover. São uma segunda fonte de verdade divergente (Python 3.13 × 3.12) e não são usados pela imagem.
- **`docker-compose.yml`:** fixar as tags `dpage/pgadmin4:8.14` e `ghcr.io/dbt-labs/dbt-postgres:1.8.2`.
  - ⚠️ **Desvio na execução:** a imagem `minio/minio` deixou de ser distribuída publicamente (Docker Hub e Quay exigem autenticação), então `minio/minio:latest` falharia num clone novo. Substituída por `bitnamilegacy/minio:2025.7.23-debian-12-r5`, o último build público versionado. O volume passa para `/bitnami/minio/data` e o `command` deixa de ser necessário. A imagem é congelada (sem atualizações de segurança): adequado para ambiente local, mas fica registrado como risco.
- **`.env.example`:** remover `AIRFLOW_IMAGE_NAME=apache/airflow:2.7.1`, que não é usado e diverge do Dockerfile.
- **`dags/etl_pipeline.py`:** `DBT_IMAGE` passa a vir de variável de ambiente, com a mesma tag como default (uma única fonte).
- **Validação:** `docker compose build --no-cache` conclui e `docker compose run --rm airflow-webserver airflow version` retorna `2.9.0`.

#### INF-02 · Remover segredos hardcoded
- **`docker-compose.yml`:** passam a vir do `.env` a `AIRFLOW__CORE__FERNET_KEY`, o usuário e a senha do `postgres-airflow` (inclusive na `SQL_ALCHEMY_CONN`), as credenciais admin do Airflow e do pgAdmin e o `MINIO_ROOT_USER/PASSWORD`. O MinIO passa a reutilizar `AWS_ACCESS_KEY_ID/SECRET`, para que exista uma fonte só.
- **Scripts Python:** remover os defaults `"minio"`/`"minio123"`, `"postgres"` etc. Credencial ausente gera erro explícito. Nesta sprint isso é feito com um helper local mínimo; a centralização fica para o ENG-01 (Sprint 2).
- **`.env.example`:** listar todas as variáveis, com placeholders e instrução para gerar a Fernet key (`python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`).
- **Validação:** `grep -rnE "minio123|admin@admin|wYhQ2F" --include=*.py --include=*.yml .` não retorna nada.

#### INF-03 · Limpar serviços e artefatos sem uso
- Remover o serviço `dbt-core` do compose.
- Remover `init/01_schema.sql` e o volume `./init` do `postgres-gold`.
- **Rede determinística:** `networks.uberlandia-net.name: uberlandia-net`. O default `refresco-data-case_uberlandia-net` da DAG depende do nome da pasta e quebra se o repositório for clonado com outro nome.
- **GID:** `group_add: ["${DOCKER_GID:-984}"]`, documentado no `.env.example` (`stat -c '%g' /var/run/docker.sock`).
- **`docker.sock`:** manter com leitura e escrita e **corrigir o README**. Montar o socket como `:ro` não impede chamadas à API do Docker, então daria uma falsa sensação de segurança. Vou documentar o risco real: acesso ao socket equivale a root no host, o que é aceitável num ambiente local.
- **Validação:** `docker compose config` válido, a DAG resolve a rede `uberlandia-net` e `dbt_deps` executa.

### Fase 2: Bronze

#### DQ-01 · Encoding na ingestão
- **`pipeline/bronze/ingest_csv.py`:** mapa explícito `SOURCE_ENCODINGS = {"clientes": "latin1", "vendas": "utf-8", "produtos": "utf-8", "financeiro": "utf-8"}`, no lugar do `latin1` fixo. A Bronze é sempre gravada em UTF-8 (`encoding="utf-8"` explícito). Usar o mapa explícito em vez de detecção automática deixa o comportamento previsível e o contrato da fonte documentado.
- **`clean_produtos.py` / `clean_financeiro.py`:** remover `fix_encoding`, que vira desnecessária. Manter o `strip`/`upper`.
  - ⚠️ **Desvio na execução:** ao ler cada arquivo com o encoding correto, `clientes.csv` ainda tinha **4 nomes com dupla codificação na própria origem** (ex.: `Mercado do ZÃ©`, `Padaria da Maria CÃ£o`, C0201). Para esses foi criada `repair_double_encoding` em `clean_clientes.py`. Ela só atua quando encontra marcadores de mojibake (`Ã`, `Â`), mantém o valor original se a conversão falhar e registra no log a quantidade corrigida. A Bronze continua fiel à origem.
  - Corrigido também o `KeyError: 'mtime'` na listagem de arquivos da Bronze (objetos S3 expõem `LastModified`). Era uma falha antiga, só de log.
- **Validação:**
  - `select distinct canal_venda from gold.dim_representante` deve retornar `Conveniência`.
  - `select distinct familia from gold.dim_produto` e `select distinct tipo_lancamento from gold.fato_vendas` não devem ter `Ã`.

#### DQ-02 · Coordenadas e parâmetros do clima
- **`pipeline/bronze/ingest_clima.py`:**
  - Usar `CLIMA_LATITUDE=-18.9186` e `CLIMA_LONGITUDE=-48.2772` (Uberlândia), além de `CLIMA_ANO_INICIO=2020` e `CLIMA_ANO_FIM=2025`, todos via env, com defaults no `.env.example`. O período atual é mantido para não mudar o comportamento.
  - Remover `API_URL`, que não é usado.
  - Adicionar `timeout=30` no `requests.get` e `raise_for_status()`.
  - `fetch_weather_data_by_year` deixa de engolir exceções com `return None`: falha parcial passa a ser `logger.warning` com a contagem final de anos obtidos.
- **`.env.example`:** remover `OPEN_METEO_API_URL` (forecast), que não é usado. A URL de arquivo passa a ser `OPEN_METEO_ARCHIVE_URL`.
- **Fora do escopo:** clima por região (os clientes estão em 4 mesorregiões de MG). Fica registrado como card futuro no backlog.
- **Validação:** o parâmetro `latitude=-18.9186` aparece no log da task, e a temperatura máxima média de 2024 fica coerente com Uberlândia (~29–30 °C).

### Fase 3: Silver

#### DQ-03 · Eliminar a string `"nan"`
- Trocar `astype(str)` por `astype("string")` (dtype nullable do pandas) em `clean_clientes.py`, `clean_produtos.py`, `clean_financeiro.py` e `clean_vendas.py`. Strings vazias depois do `strip` viram `<NA>`.
- **`clean_financeiro.py`:** remover o `.replace("NAN", np.nan)`, que vira desnecessário, e corrigir o `dropna(inplace=True)` sobre um slice (`.copy()`), eliminando o `SettingWithCopyWarning`.
- **Canal ausente (3 clientes):** permanece `NULL` na Silver. A `dim_cliente` aplica `coalesce(canal, 'Não informado')`, o padrão Kimball de membro desconhecido.
- **`vw_q2_volume_vs_meta.sql`:** remover o `WHEN c.canal = 'nan'` e o filtro `c.canal IS NOT NULL`.
- **Validação:** `select count(*) from load_stage.clientes where canal = 'nan' or nome_cliente = 'nan'` deve retornar 0, e `select canal, count(*) from gold.dim_cliente group by 1` deve mostrar `Não informado = 3`.

#### DQ-08 · Deduplicar clientes pela chave natural *(depende da D1)*
- **`clean_clientes.py`**, assumindo a opção A da D1:
  1. Deduplicar por `id_cliente` (mantém o `_ingested_at` mais recente).
  2. Para cada `cnpj` duplicado, manter o menor `id_cliente`. Os demais vão para `s3://silver/_quarentena/clientes/` com `motivo_rejeicao`.
  3. Clientes com `id_cliente` fora do padrão também passam a ir para a quarentena. Hoje eles só aparecem no log, o que antecipa parte do ARQ-05.
- **Validação:**
  - A quarentena de vendas deixa de ter `id_cliente sem cadastro: C0001..C0005`.
  - `gold.fato_vendas` ganha 147 linhas.
  - `silver/_quarentena/clientes/` tem 5 linhas.
- **Resultado da execução:**
  - Regras extraídas para a função pura `deduplicate_clientes`, com ordenação estável (resultado determinístico) e quarentena com motivo.
  - IDs padronizados em maiúsculas em clientes, produtos e vendas. A regex aceitava `c0002`, mas o join falharia.
  - **141 vendas recuperadas** (não 147). As 6 restantes de C0001–C0005 são descartadas por outro motivo, descrito abaixo.
  - ⚠️ **Achado novo:** `parse_mixed_dates` só reconhece `AAAA-MM-DD` e `DD/MM/AAAA`. Datas em `DD/MM/AA` (64) e `DD-MM-AAAA` (61) viram nulas e as **125 vendas (3,6%) são descartadas sem log nem quarentena**. Tratamento incorporado ao DQ-04.

#### DQ-04 · Validar medidas de vendas
- **`clean_vendas.py`:** generalizar a validação com uma lista de regras `(nome, máscara, mensagem)`, aplicadas junto com as regras referenciais que já existem, para manter um único ponto de quarentena:
  - `qtd_caixas > 0` (6 registros violam hoje)
  - `0 <= desconto_pct < 1` (3 violam: 2.60, 5.04, 20.73)
  - `preco_unitario > 0` (0 violam; regra preventiva)
  - Valores nulos nas três colunas também são rejeitados.
- **`dbt/models/staging/schema.yml`:** adicionar `dbt_utils.accepted_range` em `qtd_caixas` (`min_value: 1`), `desconto_pct` (`0` a `1`, exclusivo) e `preco_unitario` (`min_value: 0`, exclusivo). É uma defesa em profundidade: se a regra da Silver regredir, o dbt falha.
- **`vw_q4_margem_bruta.sql`:** remover `AND f.receita_liquida > 0`.
- **Escopo ampliado (achado do DQ-08):** `parse_mixed_dates` passa a reconhecer também `DD/MM/AA` e `DD-MM-AAAA`. Datas que continuarem inválidas vão para a quarentena com motivo, em vez de serem descartadas em silêncio.
- **Validação:** `select count(*) from gold.fato_vendas where receita_liquida <= 0` deve retornar 0 sem filtro na view, e a quarentena deve ter as 9 vendas com motivo legível. As 125 vendas com data em formato alternativo devem chegar à fato.
- **Resultado da execução:**
  - `validate_referential_integrity` virou `validate_vendas`: uma única lista de regras (obrigatórios, medidas, data e integridade referencial) e um único ponto de quarentena. O motivo junta todas as regras violadas.
  - Nada é descartado em silêncio: o `dropna` de datas inválidas e o filtro de `id_venda` vazio viraram regras de quarentena. A quarentena preserva o texto original da data (`data_emissao_original`).
  - Fato: **3.482 vendas** (3.366 + 125 datas recuperadas − 9 medidas inválidas). Quarentena de vendas: 18 (6 `qtd_caixas`, 3 `desconto_pct`, 5 sem cliente e 4 produtos inválidos).
  - dbt: 50 testes PASS, inclusive `accepted_range` em `qtd_caixas`/`preco_unitario` e `expression_is_true` em `desconto_pct`.
  - Nota técnica: `ano`/`mes` passam a ser calculados só para os aprovados, como `int64`. Usar `Int64` (nullable) em `partition_cols` quebra a releitura no pyarrow 15.

### Fase 4: dbt

#### DQ-07 · Clima no grão diário
- **`dim_tempo.sql`:**
  - Join por `d.data_completa = s.data_clima::date`, com as colunas `clima_temp_max` e `clima_precipitacao` agora diárias.
  - Agregados mensais, se forem úteis ao dashboard, ficam em `clima_temp_max_media_mes` e `clima_precipitacao_total_mes` via window function.
  - O tipo de `clima_precipitacao` muda de `numeric(5,2)` para `numeric(7,2)`, para não estourar com somas mensais.
  - Remover o comentário desatualizado ("apenas 2024").
- **`gold/schema.yml`:** recalcular `warn_if`/`error_if` (a série tem 4.018 dias e o clima cobre 2020–2025, ou seja, 2.192 dias) e corrigir as descrições.
- **Verificar antes:** se o Streamlit usa `dim_tempo` diretamente. Hoje consome só as `vw_q*`.
- **Validação:** `select count(distinct clima_precipitacao) from gold.dim_tempo where ano = 2024 and mes = 1` deve retornar mais de 1 (antes era 1 valor por mês).
- **Resultado da execução:**
  - Clima diário e as colunas `clima_temp_max_media_mes` / `clima_precipitacao_total_mes` (window functions). Jan/2024: 24 valores distintos de precipitação. A soma diária de 2024 bate com a soma dos totais mensais (1.097,9 mm).
  - Limites do teste corrigidos: a configuração anterior combinava `severity: warn` com `error_if`, então o erro nunca disparava. Agora: alerta acima de 1.826 nulos, erro se nenhum dia tiver clima.
  - Teste singular novo, `dbt/tests/assert_clima_cobre_dias_com_venda.sql`. Ele é orientado a dados (não depende de contagens fixas) e falha se algum dia com venda ficar sem clima. dbt: 51 testes PASS.

#### DQ-06 · Regra por `tipo_lancamento` *(depende da D2)*
- Criar `dbt/seeds/regra_tipo_lancamento.csv` e o seu `schema.yml`, com `unique`/`not_null` em `tipo_lancamento`.
- **`dbt_project.yml`:** configurar `seeds` no schema `gold`.
- **`fato_vendas.sql`:** left join com o seed, expondo `entra_receita` e `entra_volume`. Vendas sem lançamento ficam com as flags `false`.
- **`vw_q1`, `vw_q3`, `vw_q4`:** trocar `WHERE status = 'PAGO'` por `status = 'PAGO' AND entra_receita` (Q1 e Q4) ou `AND entra_volume` (Q3).
- **DAG:** incluir `dbt seed` (nova task `dbt_seed`, entre `dbt_deps` e `dbt_run_staging`).
- **Atenção:** a `fato_vendas` é incremental. A mudança de colunas exige `dbt run --full-refresh -s fato_vendas` uma vez. Vou documentar isso no README.
- **Validação:** `select tipo_lancamento, count(*) from gold.fato_vendas where status_financeiro='PAGO' and entra_receita group by 1` deve retornar só `VENDA`.
- **Resultado da execução:**
  - Seed `regra_tipo_lancamento` com schema YAML próprio (tipos de coluna e testes). Na `fato_vendas`, o teste `accepted_values` com lista fixa foi trocado por `relationships` com o seed, que vira a fonte única dos tipos válidos.
  - `vw_q4` segue a D2 por completo: receita só de `VENDA`, custo de `VENDA` + `BONIFICACAO` (bonificação reduz a margem).
  - Vendas `PAGO` consideradas: antes 849; agora 223 em receita e 436 em volume. Receita total na Q4: R$ 39.431,22; custo: R$ 30.764,25; margem: R$ 8.666,97.
  - DAG: task `dbt_seed` entre `dbt_deps` e `dbt_run_staging` (14 tasks). dbt: 57 testes PASS.

#### DQ-05 · Metas via seed *(depende da D3)*
- Criar `dbt/seeds/metas_canal_mes.csv` (`canal, ano, mes, meta_volume_caixas`), com um comentário no `schema.yml` indicando que os valores são fictícios.
- Testes: `dbt_utils.unique_combination_of_columns` em `(canal, ano, mes)`, `not_null` e `accepted_range` (`> 0`).
- **`vw_q2_volume_vs_meta.sql`:** reescrita.
  - O realizado (já com a regra `entra_volume` da DQ-06) é agregado por canal, com `left join` nas metas somadas por canal.
  - **O contrato de colunas é mantido** (`canal`, `volume_realizado_caixas`, `receita_realizada`, `meta_volume_caixas`, `percentual_atingimento`, `total_vendas_realizadas`), para não quebrar o `streamlit/main.py:281-317`.
- **Validação:** o `percentual_atingimento` varia entre canais e não é mais constante (`1/fator`).
- **Resultado da execução:**
  - Metas fictícias fixas por canal/mês (Atacado 220, Conveniência 170, Food Service 200, Supermercado 160 e Varejo Tradicional 180 caixas/mês em 2024), escolhidas uma vez e documentadas como fictícias no YAML do seed e no dashboard.
  - Testes do seed: combinação única (canal, ano, mês), `not_null`, faixas de `mes`/meta e `relationships` de `canal` com `dim_cliente.canal` (pega erro de grafia ou acento).
  - `vw_q2` cruza realizado e meta no grão canal × ano × mês (`FULL OUTER JOIN`), então mês sem venda conta como meta não atingida. O contrato de colunas do dashboard foi mantido.
  - Atingimento: Atacado 93,33%, Conveniência 104,22%, Food Service 100,71%, Supermercado 92,97%, Varejo Tradicional 98,56%. `Não informado` aparece sem meta.
  - Streamlit: descrição da Q2 informa que as metas são fictícias, e o rótulo da barra de meta fica vazio (não "nan") quando não há meta.
  - dbt: 2 seeds, 65 testes PASS.

### Fase 5: Documentação

#### DOC-02 · Documentos de decisão
- **`docs/decisoes_tratamento.md`:** preencher em formato ADR curto (contexto → decisão → consequências), cobrindo as decisões D1, D2, D3, o encoding por fonte e o membro desconhecido `Não informado`.
- **`docs/diagnostico_orfaos_silver.md`:** marcar como **resolvido** e registrar a descoberta da D1. Os órfãos C0004/C0005 eram gerados pelo dedup por CNPJ. Os verdadeiros órfãos da origem são `P-01`, `P000`, `P099`, `PXXX` e 5 vendas sem `id_cliente`.
- **`dags/etl_pipeline.py`:**
  - Corrigir o comentário das linhas 125–130: as views somem por causa do `DROP ... CASCADE` em `staging.py`, não do container efêmero.
  - Atualizar o diagrama ASCII das dependências, que hoje não inclui `dbt_deps`, `dbt_run_staging`, `dbt_seed` e `dbt_run_analytics`.

#### DOC-01 · README alinhado ao código
- Corrigir o diagrama ER com as colunas reais (sem `id_representante` e `receita_bruta`; incluir `representante`, `cnpj`, `trimestre`, `is_dia_util`, `entra_receita` etc.).
- **Diagrama de arquitetura:** remover o serviço `dbt-core` persistente e descrever o `docker.sock` corretamente.
- **Qualidade de dados:** descrever as quarentenas reais (vendas e clientes), as regras de medidas e o seed de regras.
- **Como executar:**
  - Remover o `airflow db init`.
  - Adicionar a configuração do `.env` (Fernet key, `DOCKER_GID`, `DBT_PROJECT_HOST_PATH`).
  - Adicionar o `--full-refresh` para quem já tinha o ambiente.
- Atualizar a estrutura de pastas (`streamlit/`, `notebooks/`, `docs/backlog/`, `dbt/seeds/`; sem `perguntas.sql` e `init/`).
- Novas seções:
  - **Regras de negócio:** tabela da D2 e metas fictícias.
  - **Limitações conhecidas.**
  - Links para `BACKLOG.md` e este plano.

---

### Resultado da execução (Fase 5)
- **DOC-02:**
  - `decisoes_tratamento.md` com 8 ADRs: encoding, nulos e membro desconhecido, quarentena, D1, D2, D3, clima e configuração.
  - Diagnóstico de órfãos marcado como resolvido, com a correção (C0004/C0005 não eram órfãos da origem) e os commits.
  - Comentário e diagrama da DAG corrigidos.
- **DOC-01:**
  - README reescrito, com arquitetura, modelo resumido, regras de negócio, qualidade, execução, estrutura, limitações conhecidas e roadmap.
  - `docs/arquitetura_dados.md` virou a referência técnica única: componentes, fluxo, DAG, ER real, views e testes. O README aponta para ele, para que os dois diagramas não divirjam de novo.

## 4. Variáveis novas no `.env`

```dotenv
# Airflow
AIRFLOW_FERNET_KEY=<gerar>
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=<definir>
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=<definir>
DOCKER_GID=984                 # stat -c '%g' /var/run/docker.sock

# pgAdmin
PGADMIN_EMAIL=<definir>
PGADMIN_PASSWORD=<definir>

# dbt
DBT_IMAGE=ghcr.io/dbt-labs/dbt-postgres:1.8.<patch>

# Clima
OPEN_METEO_ARCHIVE_URL=https://archive-api.open-meteo.com/v1/archive
CLIMA_LATITUDE=-18.9186
CLIMA_LONGITUDE=-48.2772
CLIMA_ANO_INICIO=2020
CLIMA_ANO_FIM=2025
```

Removidas: `AIRFLOW_IMAGE_NAME` e `OPEN_METEO_API_URL`.

---

## 5. Validação final da sprint

1. `docker compose down -v && docker compose build --no-cache && docker compose up -d` *(depende da D5)*
2. Executar a DAG `dag_etl_pipeline`: todas as tasks em verde, inclusive `dbt_test_gold`.
3. Checagens de dados:

| Verificação | Esperado |
|---|---|
| Caracteres `Ã` em qualquer coluna texto da Gold | 0 |
| Valores `'nan'` em `load_stage.*` | 0 |
| Linhas em `gold.fato_vendas` | antes + 147 (clientes recuperados) − 9 (medidas inválidas) |
| Linhas em `silver/_quarentena/clientes/` | 5 |
| `receita_liquida <= 0` em `gold.fato_vendas` | 0 |
| `percentual_atingimento` na `vw_q2` | varia entre canais |
| Valores distintos de clima por dia em um mês | > 1 |
| Tipos considerados em receita | apenas `VENDA` |

4. Abrir o Streamlit (`localhost:8501`): as 4 abas carregam sem erro e os números refletem as novas regras.
5. Registrar no PR os números de antes e depois (linhas da fato, receita total, quarentena por motivo).

**Definição de pronto de cada card:** critérios de aceite do `BACKLOG.md` marcados, commit próprio com o ID do card, DAG verde após a mudança e card movido para "Em Revisão" (ou "Concluído" após o merge).

---

## 6. Riscos e mitigação

| Risco | Mitigação |
|---|---|
| Tags fixas inexistentes no registry | Conferir as tags no Docker Hub/GHCR antes do commit do INF-01 |
| Incremental da fato com colunas novas | `--full-refresh` documentado. A correção estrutural é o MOD-01 (Sprint 2) |
| Queda forte nos números do dashboard (D2) | Esperada. Explicada no README e no PR com o comparativo de antes e depois |
| Sem testes automatizados nesta sprint | Validação manual pelo checklist acima. Testes unitários e CI ficam no ENG-02/ENG-03 (Sprint 2) |
| Mudança de credenciais invalida os volumes existentes | Recriação com `down -v` (D5). Os dados são reproduzíveis |

---

## 7. Fora do escopo desta sprint

- Refatoração para `pipeline/common/` (ENG-01): nesta sprint entram só os ajustes mínimos necessários.
- Testes unitários e CI (ENG-02, ENG-03).
- Correção estrutural do incremental (MOD-01) e endurecimento dos testes de relacionamento (MOD-02).
- Quarentena genérica com tabela de auditoria (ARQ-05). Só clientes ganham quarentena agora, por causa da D1.
- Clima por região: **novo card a incluir no backlog**.

---

## Aprovação

- [x] D1: tratamento de CNPJ duplicado (opção: A)
- [x] D2: regra por `tipo_lancamento`
- [x] D3: origem dos valores de meta (opção: A)
- [x] D4: estratégia de Git
- [x] D5: recriação dos volumes Docker
- [x] Plano aprovado
