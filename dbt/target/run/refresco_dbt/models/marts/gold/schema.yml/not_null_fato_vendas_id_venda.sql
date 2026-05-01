select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select id_venda
from "gold"."gold"."fato_vendas"
where id_venda is null



      
    ) dbt_internal_test