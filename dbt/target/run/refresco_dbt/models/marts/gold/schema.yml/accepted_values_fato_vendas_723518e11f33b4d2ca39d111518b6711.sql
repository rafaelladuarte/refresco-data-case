select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

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



      
    ) dbt_internal_test