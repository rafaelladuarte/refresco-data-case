with source as (
    select distinct
        nome_representante,
        canal_venda
    from {{ ref('stg_vendas') }}
    where nome_representante is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['nome_representante', 'canal_venda']) }} as sk_representante,
    nome_representante,
    canal_venda
from source
