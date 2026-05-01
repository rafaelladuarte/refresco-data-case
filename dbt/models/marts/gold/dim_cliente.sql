with source as (
    select * from {{ ref('stg_clientes') }}
),

scd1 as (
    select 
        *,
        row_number() over (partition by id_cliente order by ingested_at desc) as rn
    from source
),

latest_records as (
    select * from scd1 where rn = 1
)

select
    {{ dbt_utils.generate_surrogate_key(['id_cliente']) }} as sk_cliente,
    id_cliente,
    nome_cliente,
    cnpj,
    canal,
    cidade,
    uf,
    regiao,
    nome_representante as representante,
    is_ativo as ativo
from latest_records
