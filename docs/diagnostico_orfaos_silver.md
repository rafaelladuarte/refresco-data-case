# Diagnóstico: Registros Órfãos na Camada Silver

> ✅ **Status: RESOLVIDO na Sprint 1.** Este documento é mantido como registro histórico do diagnóstico.
> Os trechos de código e comandos abaixo refletem o estado **anterior** às correções. O serviço `dbt-core`, por exemplo, não existe mais: o dbt roda via DockerOperator.
>
> **Correção do diagnóstico.** Parte dos "órfãos" listados não vinha da origem:
> - **C0004 e C0005 existem em `clientes.csv`.** Eram descartados pelo próprio pipeline: o dedup por CNPJ mantinha os recadastros C0203/C0204 (sem vendas) e descartava os originais. Resolvido em `04d307f` ([DQ-08], ver [ADR-004](decisoes_tratamento.md#adr-004--clientes-com-cnpj-duplicado-d1)).
> - Os **órfãos reais da origem** são os produtos `P-01`, `PXXX`, `P000`, `P099` e 5 vendas sem `id_cliente`. Eles vão para `silver/_quarentena/vendas/` com motivo.
>
> **Commits relacionados:**
> - `4d3605a` [DQ-03]: a string `"nan"` não é mais gerada (`clean_text`).
> - `04d307f` [DQ-08]: dedup de clientes por `id_cliente` e quarentena de CNPJ duplicado.
> - `bed0775` [DQ-04]: `validate_referential_integrity` virou `validate_vendas` (medidas, datas e integridade), sem descarte silencioso.
>
> **Testes.** Os testes `relationships` de `sk_cliente`/`sk_produto` na Gold continuam com `severity: warn`. O endurecimento para `error` está planejado no card MOD-02 do [`BACKLOG.md`](../BACKLOG.md).

---

## Resumo Executivo

Os registros órfãos (`C0004`, `C0005`, linha em branco, `P099`, `PXXX`, `P000`, `P-01`) chegam à tabela fato porque os scripts Silver **não validam integridade referencial nem formato de IDs** antes de salvar no S3. Cada script opera em sua própria tabela de forma isolada — nenhum deles verifica se os IDs de `vendas` existem nos cadastros de `clientes` e `produtos`.

---

## Falhas identificadas por arquivo

### `clean_clientes.py` — 2 falhas

| # | Linha | Falha | Impacto |
|---|---|---|---|
| 1 | 57–58 | Filtra `cnpj` e `nome_cliente` nulos, mas **não valida `id_cliente`** | IDs como `C0004`, `C0005`, string vazia passam para a Silver |
| 2 | 51 | `df[col].astype(str).str.strip()` — se `id_cliente` for `NaN`, vira a string `"nan"` | String `"nan"` passa nos filtros downstream sem disparar alerta |

```python
# ❌ Atual — sem validação de id_cliente
df = df[df["cnpj"].notna()]
df = df[df["nome_cliente"].notna()]
# id_cliente nunca é checado

# ✅ Corrigido
df["id_cliente"] = df["id_cliente"].fillna("").astype(str).str.strip()
mask_invalido = ~df["id_cliente"].str.match(r'^C\d{4}$', re.IGNORECASE)
df = df[~mask_invalido]   # descarta IDs fora do padrão com log de aviso
```

---

### `clean_produtos.py` — 2 falhas

| # | Linha | Falha | Impacto |
|---|---|---|---|
| 1 | 56 | `drop_duplicates(subset=["id_produto"])` sem validar formato antes | `P099`, `PXXX`, `P000`, `P-01` entram na Silver como "produtos válidos" |
| 2 | 49 | `str.extract(r'(\d+)')` em `volume_ml` — não gera erro em IDs, mas silencia falhas de parsing | Produto com `volume_ml` inválido passa com `NaN` sem aviso |

```python
# ❌ Atual — dedup sem validação
df = df.sort_values("_ingested_at").drop_duplicates(subset=["id_produto"], keep="last")

# ✅ Corrigido — valida antes de deduplicar
df["id_produto"] = df["id_produto"].fillna("").astype(str).str.strip()
mask_invalido = ~df["id_produto"].str.match(r'^P\d{3}$', re.IGNORECASE)
df = df[~mask_invalido]
df = df.sort_values("_ingested_at").drop_duplicates(subset=["id_produto"], keep="last")
```

---

### `clean_vendas.py` — 3 falhas (causa raiz dos órfãos)

| # | Linha | Falha | Impacto |
|---|---|---|---|
| 1 | 53 | `dropna(subset=["data_emissao", "id_venda", "ano", "mes"])` — **`id_cliente` e `id_produto` não constam** | Vendas com IDs inválidos passam pela única barreira de qualidade |
| 2 | 51 | `drop_duplicates(subset=["id_venda"])` sem normalize anterior | Se `id_venda` vier com espaços, duplicatas passam despercebidas |
| 3 | Ausente | **Nenhuma validação referencial cruzada** com `clientes` e `produtos` | Script não sabe quais IDs existem nos cadastros — não pode rejeitar os órfãos |

```python
# ❌ Atual — filtra só campos de data, ignora integridade de chaves estrangeiras
df = df.dropna(subset=["data_emissao", "id_venda", "ano", "mes"])

# ✅ Corrigido — valida integridade referencial cruzada
df_clientes = pd.read_parquet("s3://silver/clientes/", storage_options=storage_options)
df_produtos = pd.read_parquet("s3://silver/produtos/produtos_clean.parquet", ...)
df_ok, df_quarentena = validate_referential_integrity(df, df_clientes, df_produtos)
```

---

### `staging.py` — 1 falha de design

| # | Linha | Falha | Impacto |
|---|---|---|---|
| 1 | 51 | `if_exists="replace"` sem transação explícita por tabela | Se a carga falhar na tabela 3 de 5, as 2 anteriores já foram substituídas — estado inconsistente no banco |

> **Nota:** O `staging.py` usa `engine.begin()` com `DROP TABLE CASCADE` antes de cada carga. Se uma tabela falhar a meio, o schema fica em estado parcialmente atualizado. Considere uma transação envolvendo todas as tabelas, ou adotar `if_exists="append"` com truncate explícito dentro da mesma transação.

---

## Arquitetura da solução corrigida

```
Bronze (CSV) 
    │
    ▼
clean_clientes.py  ──validates id_cliente format──►  Silver/clientes/
clean_produtos.py  ──validates id_produto format──►  Silver/produtos/
    │
    ▼
clean_vendas.py  
    ├── normalize IDs (strip + fillna)
    ├── load Silver/clientes → lookup set
    ├── load Silver/produtos → lookup set
    ├── validate_referential_integrity()
    │       ├── Regra 1: id vazio → rejeita
    │       ├── Regra 2: formato inválido (regex) → rejeita  
    │       ├── Regra 3: ID não existe no cadastro → rejeita
    │       └── motivo_rejeicao anotado por linha
    │
    ├──[PASS]──► Silver/vendas/          (apenas dados íntegros)
    └──[FAIL]──► Silver/_quarentena/vendas/  (auditoria + reprocessamento)
```

---

## Testes dbt que detectariam isso automaticamente

Esses testes já existem no `schema.yml` após as correções anteriores:

```yaml
# gold/schema.yml — já configurados como severity: warn
- name: sk_cliente
  data_tests:
    - not_null:
        config:
          severity: warn   # dispara aviso quando vendas não encontram cliente
    - relationships:
        to: ref('dim_cliente')
        field: sk_cliente
        config:
          severity: warn
```

**Com as correções Python aplicadas, esses warns devem desaparecer nas próximas execuções** — pois os registros com IDs inválidos serão descartados na camada Silver antes de chegar ao `load_stage`.

---

## Checklist de validação pós-correção

```bash
# 1. Rodar os scripts Silver na ordem correta
#    (clientes e produtos ANTES de vendas, pois vendas faz lookup neles)
python pipeline/silver/clean_clientes.py
python pipeline/silver/clean_produtos.py
python pipeline/silver/clean_vendas.py

# 2. Verificar se quarentena foi gerada
#    (se o arquivo existir, há órfãos — inspecione os motivos)
# mc ls minio/silver/_quarentena/vendas/

# 3. Recarregar o load_stage
python pipeline/load/staging.py

# 4. Re-executar dbt
docker exec dbt-core dbt run --profiles-dir /usr/app/dbt
docker exec dbt-core dbt test --select marts.gold --profiles-dir /usr/app/dbt

# Resultado esperado: WARN 0 nos testes de sk_cliente e sk_produto
```
