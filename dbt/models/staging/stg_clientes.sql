with source as (
    select * from {{ source('load_stage', 'clientes') }}
),

renamed as (
    select
        -- ids
        cast(id_cliente as text) as id_cliente,
        cast(cnpj as text) as cnpj,
        
        -- dimensões
        cast(nome_cliente as text) as nome_cliente,
        cast(canal as text) as canal,
        cast(cidade as text) as cidade,
        cast(uf as text) as uf,
        cast(regiao as text) as regiao,
        cast(representante as text) as nome_representante,
        
        -- status
        cast(ativo as boolean) as is_ativo,
        
        -- metadados
        cast(_ingested_at as timestamp) as ingested_at,
        cast(_source_file as text) as source_file

    from source
)

select * from renamed
