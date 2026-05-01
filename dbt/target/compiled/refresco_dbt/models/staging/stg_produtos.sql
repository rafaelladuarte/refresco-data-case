with source as (
    select * from "gold"."load_stage"."produtos"
),

renamed as (
    select
        -- ids
        cast(id_produto as text) as id_produto,
        
        -- dimensões
        cast(nome_produto as text) as nome_produto,
        cast(familia as text) as familia,
        cast(marca as text) as marca,
        cast(embalagem as text) as embalagem,
        
        -- métricas
        cast(volume_ml as numeric(10,2)) as volume_ml,
        cast(preco_tabela as numeric(10,2)) as preco_tabela,
        cast(custo_unitario as numeric(10,2)) as custo_unitario,
        
        -- metadados
        cast(_ingested_at as timestamp) as ingested_at,
        cast(_source_file as text) as source_file

    from source
)

select * from renamed