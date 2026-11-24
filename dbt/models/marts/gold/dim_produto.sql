with source as (
    select * from {{ ref('stg_produtos') }}
),

scd1 as (
    select 
        *,
        row_number() over (partition by id_produto order by ingested_at desc) as rn
    from source
),

latest_records as (
    select * from scd1 where rn = 1
)

select
    {{ dbt_utils.generate_surrogate_key(['id_produto']) }} as sk_produto,
    id_produto,
    nome_produto,
    familia,
    marca,
    embalagem,
    volume_ml,
    preco_tabela,
    custo_unitario,
    custo_disponivel,
    preco_disponivel
from latest_records
