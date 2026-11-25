{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='id_venda',
        on_schema_change='append_new_columns'
    )
}}

{#-
  Incremental por MUDANÇA, não por data de emissão (MOD-01).

  Uma venda é (re)processada quando:
    1. a venda ou o lançamento financeiro dela mudou depois da última carga da fato
       (`atualizado_em` > max(atualizado_em) já gravado). Cobre vendas retroativas, novas ou
       corrigidas, e mudanças de status financeiro em vendas antigas; ou
    2. ela é recente: `data_emissao` dentro da janela `lookback_days` (padrão 30 dias). Cobre mudanças
       nas DIMENSÕES (ex.: o custo de um produto), que não alteram a venda e por isso não aparecem em
       `atualizado_em`. Mudança de dimensão em venda mais antiga que a janela só chega com
       `dbt run --full-refresh --select fato_vendas+` ou com `--vars '{lookback_days: <dias>}'`.

  Numa fato criada antes deste modelo ganhar `atualizado_em`, a coluna não existe: o filtro é omitido
  e tudo é reprocessado uma vez (o `append_new_columns` cria a coluna).
-#}

with vendas as (
    select * from {{ ref('stg_vendas') }}
),

financeiro as (
    select * from {{ ref('stg_financeiro') }}
),

clientes as (
    select * from {{ ref('dim_cliente') }}
),

produtos as (
    select * from {{ ref('dim_produto') }}
),

representantes as (
    select * from {{ ref('dim_representante') }}
),

regras_tipo as (
    select * from {{ ref('regra_tipo_lancamento') }}
),

vendas_com_financeiro as (
    select
        v.*,
        f.tipo_lancamento,
        f.status_financeiro,
        -- a venda vale a mais recente entre a mudança dela e a do lançamento
        greatest(v.changed_at, coalesce(f.changed_at, v.changed_at)) as atualizado_em
    from vendas v
    -- LEFT JOIN intencional: nem toda venda possui lançamento financeiro.
    -- Vendas sem correspondente em stg_financeiro = pendentes de lançamento.
    -- Não usar INNER JOIN aqui; faria sumir ~30% das vendas da fato.
    left join financeiro f
        on v.id_venda = f.id_venda
)

select
    {{ dbt_utils.generate_surrogate_key(['v.id_venda']) }} as sk_venda,
    v.id_venda,

    -- chaves estrangeiras
    c.sk_cliente,
    p.sk_produto,
    cast(to_char(v.data_emissao, 'YYYYMMDD') as integer) as sk_tempo,
    r.sk_representante,

    -- métricas e calculados
    v.qtd_caixas,
    v.preco_unitario,
    v.desconto_pct,
    v.receita_liquida,
    -- NULL quando o produto não tem custo (dim_produto.custo_disponivel = false, DQ-09):
    -- receita e volume continuam valendo, só a margem não é calculada
    (v.qtd_caixas * p.custo_unitario) as custo_total,
    (v.receita_liquida - (v.qtd_caixas * p.custo_unitario)) as margem_bruta,

    -- status financeiro
    -- NULL esperado: vendas pendentes de lançamento não possuem registro em stg_financeiro.
    -- Investigado e confirmado como comportamento de negócio (2024-05-01).
    -- ~30% das vendas (921/3375) estão nessa condição, distribuídas uniformemente por mês.
    v.tipo_lancamento,
    v.status_financeiro,

    -- regra de negócio (seed regra_tipo_lancamento); venda sem lançamento não entra
    coalesce(rt.entra_receita, false) as entra_receita,
    coalesce(rt.entra_volume, false) as entra_volume,

    -- controle
    v.data_emissao,
    v.source_file,
    v.ingested_at,
    v.atualizado_em

from vendas_com_financeiro v
left join clientes c
    on v.id_cliente = c.id_cliente
left join produtos p
    on v.id_produto = p.id_produto
left join representantes r
    on v.nome_representante = r.nome_representante
    and v.canal_venda = r.canal_venda
left join regras_tipo rt
    on v.tipo_lancamento = rt.tipo_lancamento

{% if is_incremental() %}
{#- Fato criada antes do MOD-01 ainda não tem `atualizado_em`: sem o filtro, tudo é reprocessado uma vez e
    o `append_new_columns` cria a coluna. (Referenciá-la na subconsulta falharia: o Postgres a leria como
    coluna da consulta externa.) -#}
{% set colunas_da_fato = adapter.get_columns_in_relation(this) | map(attribute='name') | list %}
{% if 'atualizado_em' in colunas_da_fato %}
where v.atualizado_em > (select coalesce(max(atualizado_em), '1900-01-01') from {{ this }})
   or v.data_emissao >= current_date - {{ var('lookback_days', 30) }}
{% endif %}
{% endif %}
