# Arquitetura e modelo de dados

Referência técnica do pipeline: componentes, fluxo entre camadas, orquestração e modelo dimensional.
O panorama geral está no [README](../README.md); as justificativas das decisões, em [`decisoes_tratamento.md`](decisoes_tratamento.md).

## 1. Componentes

| Componente | Imagem / tecnologia | Papel |
|---|---|---|
| Airflow (webserver, scheduler, init) | `apache/airflow:2.9.0-python3.12` + `requirements.txt` (constraints oficial) | Orquestra a DAG `dag_etl_pipeline`. Executa os scripts Python (Bronze, Silver, Load) com `PythonOperator` e o dbt com `DockerOperator`. |
| `postgres-airflow` | `postgres:15` | Banco de metadados do Airflow. |
| MinIO | `bitnamilegacy/minio:2025.7.23-debian-12-r5` | Object storage S3-compatível: buckets `bronze` (CSV bruto) e `silver` (Parquet + quarentena). |
| `postgres-gold` | `postgres:15` | Data warehouse: schemas `load_stage` (carga da Silver + views `stg_*`) e `gold` (dimensões, fato, seeds e views analíticas). |
| dbt (efêmero) | `ghcr.io/dbt-labs/dbt-postgres:1.8.2` | Container criado pelo `DockerOperator` a cada task dbt, na rede `uberlandia-net`, e removido ao final. |
| Streamlit | `python:3.12-slim` (`streamlit/`) | Dashboard que lê as views `gold.vw_q1..vw_q4`. |
| pgAdmin | `dpage/pgadmin4:8.14` | Exploração do warehouse (opcional). |

**`docker.sock`.** É montado no Airflow para o `DockerOperator` criar os containers dbt. Acesso ao socket equivale a root no host, e montá-lo como `:ro` não restringe a API. Isso é aceitável apenas em ambiente local.

## 2. Fluxo de dados

```mermaid
flowchart LR
    subgraph Fontes
        CSV["data/raw/*.csv<br/>clientes · produtos · vendas · financeiro"]
        API["Open-Meteo Archive API<br/>clima diário de Uberlândia"]
    end

    subgraph MinIO
        BR["bronze/<br/>CSV UTF-8 + _ingested_at"]
        SV["silver/<br/>Parquet particionado"]
        QT["silver/_quarentena/<br/>clientes · vendas"]
    end

    subgraph PG["PostgreSQL (postgres-gold)"]
        LS["load_stage<br/>tabelas + views stg_*"]
        GD["gold<br/>dim_* · fato_vendas · seeds"]
        VW["gold.vw_q1..q4"]
    end

    CSV -->|ingest_csv| BR
    API -->|ingest_clima| BR
    BR -->|clean_*| SV
    BR -.->|rejeitados| QT
    SV -->|load_to_staging| LS
    LS -->|dbt| GD
    GD -->|dbt| VW
    VW --> ST["Streamlit"]
```

| Camada | Onde | O que acontece |
|---|---|---|
| **Bronze** | `pipeline/bronze/` → `s3://bronze/<tabela>/ingestion_date=<ts>/` | Cópia fiel da origem, lida com o encoding de cada fonte e gravada em UTF-8, com `_ingested_at` e `_source_file`/`_api_source`. |
| **Silver** | `pipeline/silver/` → `s3://silver/<tabela>/` | Tipagem, nulos tratados (`clean_text`), padronização de domínios, deduplicação determinística e validação. Rejeitados vão para `_quarentena/` com `motivo_rejeicao`. |
| **Load** | `pipeline/load/staging.py` → `load_stage.*` | Carga integral da Silver no PostgreSQL. |
| **Gold** | `dbt/` → `gold.*` | Views `stg_*` (cast e renomeação), modelo dimensional, seeds de regras e views analíticas. |

## 3. Orquestração

```
ingest_csv ──► clean_clientes ───┐
           ──► clean_produtos ───┴──► clean_vendas ──┐
           ──► clean_financeiro ─────────────────────┤
ingest_clima ──► clean_clima ────────────────────────┤
                                                     ▼
                                            load_to_staging
                                                     │
 dbt_deps ► dbt_seed ► dbt_run_staging ► dbt_run_gold ► dbt_test_gold ► dbt_run_analytics
```

- `clean_vendas` depende de clientes e produtos porque faz a validação referencial contra os cadastros já limpos.
- `dbt_run_staging` roda a cada execução porque o `DROP TABLE ... CASCADE` da carga remove as views `stg_*` (melhoria prevista no card ARQ-03).
- `dbt_test_gold` bloqueia as views analíticas se algum teste com severidade `error` falhar.

## 4. Modelo dimensional (schema `gold`)

```mermaid
erDiagram
    fato_vendas }o--|| dim_cliente : sk_cliente
    fato_vendas }o--|| dim_produto : sk_produto
    fato_vendas }o--|| dim_tempo : sk_tempo
    fato_vendas }o--|| dim_representante : sk_representante
    fato_vendas }o--o| regra_tipo_lancamento : tipo_lancamento
    metas_canal_mes }o--|| dim_cliente : canal

    fato_vendas {
        text sk_venda PK "md5(id_venda)"
        text id_venda "chave natural, unique_key do incremental"
        text sk_cliente FK
        text sk_produto FK
        int sk_tempo FK "YYYYMMDD"
        text sk_representante FK
        int qtd_caixas
        numeric preco_unitario
        numeric desconto_pct "0 a 1"
        numeric receita_liquida "qtd x preco x (1 - desconto)"
        numeric custo_total "qtd x custo_unitario"
        numeric margem_bruta
        text tipo_lancamento "NULL = sem lançamento"
        text status_financeiro
        boolean entra_receita
        boolean entra_volume
        timestamp data_emissao
        text source_file
        timestamp ingested_at
    }

    dim_cliente {
        text sk_cliente PK
        text id_cliente
        text nome_cliente
        text cnpj
        text canal "'Não informado' se ausente"
        text cidade
        text uf
        text regiao
        text representante
        boolean ativo
    }

    dim_produto {
        text sk_produto PK
        text id_produto
        text nome_produto
        text familia
        text marca
        text embalagem
        numeric volume_ml
        numeric preco_tabela "NULL se indisponível"
        numeric custo_unitario "NULL se indisponível"
        boolean custo_disponivel
        boolean preco_disponivel
    }

    dim_representante {
        text sk_representante PK "md5(nome, canal)"
        text nome_representante
        text canal_venda
    }

    dim_tempo {
        int sk_tempo PK
        date data_completa
        int ano
        int mes
        int dia
        int trimestre
        text nome_mes
        text dia_semana
        boolean is_dia_util
        numeric clima_temp_max "do dia"
        numeric clima_precipitacao "do dia"
        numeric clima_temp_max_media_mes
        numeric clima_precipitacao_total_mes
    }

    regra_tipo_lancamento {
        text tipo_lancamento PK
        boolean entra_receita
        boolean entra_volume
        text descricao
    }

    metas_canal_mes {
        text canal
        int ano
        int mes
        numeric meta_volume_caixas "FICTÍCIA"
    }
```

**Notas de modelagem:**
- **Grão da fato:** uma linha por venda (`id_venda`). O lançamento financeiro entra por LEFT JOIN: ~30% das vendas não têm lançamento, então `tipo_lancamento`/`status_financeiro` ficam NULL e as flags ficam `false`.
- **Surrogate keys:** hash MD5 (`dbt_utils.generate_surrogate_key`) da chave natural, exceto `sk_tempo`, que é `YYYYMMDD`. As chaves são estáveis entre execuções.
- **Dimensões:** SCD tipo 1 (estado mais recente). Histórico de clientes (SCD2) está no backlog (MOD-05).
- **`dim_representante`** é derivada das vendas por (nome, canal). A revisão está no backlog (MOD-03).
- **`fato_vendas`** é incremental (`unique_key = id_venda`). O filtro atual por `data_emissao` não reprocessa atualizações tardias (card MOD-01).

## 5. Views analíticas

Todas consideram apenas vendas com `status_financeiro = 'PAGO'` e aplicam as flags do seed `regra_tipo_lancamento`.

| View | Pergunta | Regra aplicada |
|---|---|---|
| `vw_q1_ranking_vendas` | Top 10 produtos por receita em cada região (clientes ativos) | `entra_receita` |
| `vw_q2_volume_vs_meta` | Volume realizado vs. meta por canal | volume com `entra_volume`, receita com `entra_receita`, meta do seed `metas_canal_mes` |
| `vw_q3_tendencia_volume` | Volume diário por produto, dia anterior e média móvel de 7 dias | `entra_volume` |
| `vw_q4_margem_bruta` | Margem bruta por produto | receita de `entra_receita`, custo de `entra_volume` |

## 6. Testes de dados (dbt)

65 testes no total:
- **Staging:** `not_null`/`unique` de chaves; faixas de `qtd_caixas`, `preco_unitario` e `desconto_pct`.
- **Gold:** unicidade e `not_null` das chaves; `relationships` da fato com as dimensões e com o seed de regras; `accepted_values` de status; cobertura de clima.
- **Seeds:** unicidade, domínios e `relationships` de `metas_canal_mes.canal` com `dim_cliente`.
- **Teste singular** `dbt/tests/assert_clima_cobre_dias_com_venda.sql`: todo dia com venda tem clima.
