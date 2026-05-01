
    
    

with child as (
    select sk_representante as from_field
    from "gold"."gold"."fato_vendas"
    where sk_representante is not null
),

parent as (
    select sk_representante as to_field
    from "gold"."gold"."dim_representante"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


