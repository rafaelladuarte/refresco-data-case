
    
    

select
    id_venda as unique_field,
    count(*) as n_records

from "gold"."gold"."fato_vendas"
where id_venda is not null
group by id_venda
having count(*) > 1


