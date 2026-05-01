with source as (
    select * from "gold"."load_stage"."vendas"
),

renamed as (
    select
        -- ids
        cast(id_venda as text) as id_venda,
        cast(id_cliente as text) as id_cliente,
        cast(id_produto as text) as id_produto,
        
        -- dimensões
        cast(canal_venda as text) as canal_venda,
        cast(representante as text) as nome_representante,
        
        -- datas
        cast(data_emissao as timestamp) as data_emissao,
        cast(ano as integer) as ano_venda,
        cast(mes as integer) as mes_venda,
        
        -- métricas
        cast(qtd_caixas as integer) as qtd_caixas,
        cast(preco_unitario as numeric(10,2)) as preco_unitario,
        cast(desconto_pct as numeric(6,4)) as desconto_pct,
        cast(receita_liquida as numeric(12,2)) as receita_liquida,
        
        -- metadados
        cast(_ingested_at as timestamp) as ingested_at,
        cast(_source_file as text) as source_file

    from source
)

select * from renamed