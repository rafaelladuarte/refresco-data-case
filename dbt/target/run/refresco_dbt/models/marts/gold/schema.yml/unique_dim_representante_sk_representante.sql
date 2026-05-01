select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    sk_representante as unique_field,
    count(*) as n_records

from "gold"."gold"."dim_representante"
where sk_representante is not null
group by sk_representante
having count(*) > 1



      
    ) dbt_internal_test