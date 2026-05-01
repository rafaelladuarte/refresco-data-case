select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

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



      
    ) dbt_internal_test