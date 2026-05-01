
    
    

with child as (
    select sk_tempo as from_field
    from "gold"."gold"."fato_vendas"
    where sk_tempo is not null
),

parent as (
    select sk_tempo as to_field
    from "gold"."gold"."dim_tempo"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


