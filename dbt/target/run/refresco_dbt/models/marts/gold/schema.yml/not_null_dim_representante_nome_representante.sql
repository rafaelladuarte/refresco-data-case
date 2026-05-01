select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select nome_representante
from "gold"."gold"."dim_representante"
where nome_representante is null



      
    ) dbt_internal_test