with source as (
    select * from "gold"."load_stage"."financeiro"
),

renamed as (
    select
        -- ids
        cast(id_financeiro as text) as id_financeiro,
        cast(id_venda as text) as id_venda,
        
        -- dimensões
        cast(tipo_lancamento_padrao as text) as tipo_lancamento,
        cast(status_padrao as text) as status_financeiro,
        
        -- datas
        cast(competencia as timestamp) as data_competencia,
        cast(ano as integer) as ano_competencia,
        cast(mes as integer) as mes_competencia

    from source
)

select * from renamed