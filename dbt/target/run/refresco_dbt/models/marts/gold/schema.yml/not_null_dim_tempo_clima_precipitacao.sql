select
      count(*) as failures,
      count(*) >3653 as should_warn,
      count(*) >4018 as should_error
    from (
      
    
    



select clima_precipitacao
from "gold"."gold"."dim_tempo"
where clima_precipitacao is null



      
    ) dbt_internal_test