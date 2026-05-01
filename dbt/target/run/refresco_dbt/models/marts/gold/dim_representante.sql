
  
    

  create  table "gold"."gold"."dim_representante__dbt_tmp"
  
  
    as
  
  (
    with source as (
    select distinct
        nome_representante,
        canal_venda
    from "gold"."load_stage"."stg_vendas"
    where nome_representante is not null
)

select
    md5(cast(coalesce(cast(nome_representante as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(canal_venda as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as sk_representante,
    nome_representante,
    canal_venda
from source
  );
  