select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select sk_tempo
from "gold"."gold"."fato_vendas"
where sk_tempo is null



      
    ) dbt_internal_test