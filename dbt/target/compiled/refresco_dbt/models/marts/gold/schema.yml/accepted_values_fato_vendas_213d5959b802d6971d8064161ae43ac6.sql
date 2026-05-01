
    
    

with all_values as (

    select
        tipo_lancamento as value_field,
        count(*) as n_records

    from "gold"."gold"."fato_vendas"
    group by tipo_lancamento

)

select *
from all_values
where value_field not in (
    'VENDA','BONIFICACAO','DEVOLUCAO','AJUSTE','OUTRO'
)


