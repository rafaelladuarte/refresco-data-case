
    
    

with all_values as (

    select
        status_financeiro as value_field,
        count(*) as n_records

    from "gold"."gold"."fato_vendas"
    group by status_financeiro

)

select *
from all_values
where value_field not in (
    'PAGO','PENDENTE','CANCELADO'
)


