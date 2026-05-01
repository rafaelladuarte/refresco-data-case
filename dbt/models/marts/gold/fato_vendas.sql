{{
    config(
        materialized='incremental',
        unique_key='id_venda'
    )
}}

with vendas as (
    select * from {{ ref('stg_vendas') }}
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where data_emissao > (select coalesce(max(data_emissao), '1900-01-01') from {{ this }})
    {% endif %}
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
    (v.qtd_caixas * p.custo_unitario) as custo_total,
    (v.receita_liquida - (v.qtd_caixas * p.custo_unitario)) as margem_bruta,
    
    -- status financeiro
    -- NULL esperado: vendas pendentes de lançamento não possuem registro em stg_financeiro.
    -- Investigado e confirmado como comportamento de negócio (2024-05-01).
    -- ~30% das vendas (921/3375) estão nessa condição, distribuídas uniformemente por mês.
    f.tipo_lancamento,
    f.status_financeiro,
    
    -- controle
    v.data_emissao,
    v.source_file,
    v.ingested_at

from vendas v
left join clientes c 
    on v.id_cliente = c.id_cliente
left join produtos p 
    on v.id_produto = p.id_produto
left join representantes r 
    on v.nome_representante = r.nome_representante 
    and v.canal_venda = r.canal_venda
-- LEFT JOIN intencional: nem toda venda possui lançamento financeiro.
-- Vendas sem correspondente em stg_financeiro = pendentes de lançamento.
-- Não usar INNER JOIN aqui; faria sumir ~30% das vendas da fato.
left join financeiro f 
    on v.id_venda = f.id_venda
