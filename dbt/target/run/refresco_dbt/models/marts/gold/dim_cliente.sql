
  
    

  create  table "gold"."gold"."dim_cliente__dbt_tmp"
  
  
    as
  
  (
    with source as (
    select * from "gold"."load_stage"."stg_clientes"
),

scd1 as (
    select 
        *,
        row_number() over (partition by id_cliente order by ingested_at desc) as rn
    from source
),

latest_records as (
    select * from scd1 where rn = 1
)

select
    md5(cast(coalesce(cast(id_cliente as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as sk_cliente,
    id_cliente,
    nome_cliente,
    cnpj,
    canal,
    cidade,
    uf,
    regiao,
    nome_representante as representante,
    is_ativo as ativo
from latest_records
  );
  