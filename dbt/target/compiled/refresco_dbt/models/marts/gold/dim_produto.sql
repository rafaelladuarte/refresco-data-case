with source as (
    select * from "gold"."load_stage"."stg_produtos"
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
    md5(cast(coalesce(cast(id_produto as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as sk_produto,
    id_produto,
    nome_produto,
    familia,
    marca,
    embalagem,
    volume_ml,
    preco_tabela,
    custo_unitario
from latest_records