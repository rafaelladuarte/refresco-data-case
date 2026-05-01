select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select sk_produto as from_field
    from "gold"."gold"."fato_vendas"
    where sk_produto is not null
),

parent as (
    select sk_produto as to_field
    from "gold"."gold"."dim_produto"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test