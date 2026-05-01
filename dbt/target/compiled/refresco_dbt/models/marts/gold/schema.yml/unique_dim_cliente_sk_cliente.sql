
    
    

select
    sk_cliente as unique_field,
    count(*) as n_records

from "gold"."gold"."dim_cliente"
where sk_cliente is not null
group by sk_cliente
having count(*) > 1


