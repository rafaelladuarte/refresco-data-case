/*
  dim_tempo — calendário diário (2020-01-01 → 2030-12-31, 4018 dias) enriquecido com clima.

  Clima:
    - clima_temp_max / clima_precipitacao: valores DO DIA (join por data).
    - *_mes: agregados do mês, repetidos em cada dia do mês, para análises mensais
      sem precisar reagregar na consulta.
    - Dias fora do período ingerido (CLIMA_ANO_INICIO..CLIMA_ANO_FIM, padrão 2020–2025)
      ficam com clima NULL. O LEFT JOIN é intencional: o calendário não depende do clima.
*/
with date_series as (
    select generate_series(
        '2020-01-01'::date,
        '2030-12-31'::date,
        '1 day'::interval
    )::date as data_completa
),

dim_dates as (
    select
        cast(to_char(data_completa, 'YYYYMMDD') as integer) as sk_tempo,
        data_completa,
        cast(extract(year from data_completa) as integer) as ano,
        cast(extract(month from data_completa) as integer) as mes,
        cast(extract(day from data_completa) as integer) as dia,
        cast(extract(quarter from data_completa) as integer) as trimestre,
        case extract(month from data_completa)
            when 1 then 'Janeiro' when 2 then 'Fevereiro' when 3 then 'Março'
            when 4 then 'Abril' when 5 then 'Maio' when 6 then 'Junho'
            when 7 then 'Julho' when 8 then 'Agosto' when 9 then 'Setembro'
            when 10 then 'Outubro' when 11 then 'Novembro' when 12 then 'Dezembro'
        end as nome_mes,
        case extract(isodow from data_completa)
            when 1 then 'Segunda-feira' when 2 then 'Terça-feira' when 3 then 'Quarta-feira'
            when 4 then 'Quinta-feira' when 5 then 'Sexta-feira' when 6 then 'Sábado'
            when 7 then 'Domingo'
        end as dia_semana,
        case
            when extract(isodow from data_completa) in (6, 7) then false
            else true
        end as is_dia_util
    from date_series
),

clima_diario as (
    select
        cast(data_clima as date) as data_clima,
        temperatura_maxima,
        precipitacao_total
    from {{ ref('stg_clima') }}
),

dim_com_clima as (
    select
        d.*,
        c.temperatura_maxima as clima_temp_max,
        c.precipitacao_total as clima_precipitacao
    from dim_dates d
    left join clima_diario c
        on d.data_completa = c.data_clima
)

select
    sk_tempo,
    data_completa,
    ano,
    mes,
    dia,
    trimestre,
    nome_mes,
    dia_semana,
    is_dia_util,
    cast(clima_temp_max as numeric(5,2)) as clima_temp_max,
    cast(clima_precipitacao as numeric(6,2)) as clima_precipitacao,
    cast(avg(clima_temp_max) over (partition by ano, mes) as numeric(5,2)) as clima_temp_max_media_mes,
    cast(sum(clima_precipitacao) over (partition by ano, mes) as numeric(7,2)) as clima_precipitacao_total_mes
from dim_com_clima
