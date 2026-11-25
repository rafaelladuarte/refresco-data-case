with source as (
    select * from {{ source('load_stage', 'financeiro') }}
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
        cast(mes as integer) as mes_competencia,

        -- metadados; changed_at só avança quando o lançamento muda (ex.: status), e é o que faz
        -- uma mudança de status de uma venda antiga chegar à fato
        cast(_ingested_at as timestamp) as ingested_at,
        cast(_changed_at as timestamp) as changed_at

    from source
)

select * from renamed
