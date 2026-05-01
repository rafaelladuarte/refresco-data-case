with source as (
    select * from {{ source('load_stage', 'clima') }}
),

renamed as (
    select
        -- datas
        cast("time" as timestamp) as data_clima,
        cast(ano as integer) as ano_clima,
        cast(mes as integer) as mes_clima,
        
        -- métricas
        cast(temperature_2m_max as numeric(5,2)) as temperatura_maxima,
        cast(precipitation_sum as numeric(5,2)) as precipitacao_total,
        
        -- metadados
        cast(_ingested_at as timestamp) as ingested_at,
        cast(_api_source as text) as api_source

    from source
)

select * from renamed
