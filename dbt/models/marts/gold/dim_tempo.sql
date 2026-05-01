/*
  dim_tempo — comportamento esperado de NULL nas colunas de clima:

  A série de datas cobre 2020-01-01 → 2030-12-31 (4018 dias).
  load_stage.clima contém dados apenas do ano de 2024 (366 dias, ano bissexto).
  O LEFT JOIN por (ano, mes) retorna NULL para todos os dias fora desse período —
  esse é o comportamento correto dado o escopo atual da ingestão climática.

  Quando a ingestão de clima for ampliada para outros anos, as colunas
  clima_temp_max e clima_precipitacao serão preenchidas automaticamente
  sem nenhuma alteração neste modelo.
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

-- Agrega clima por ano+mês para fazer o join diário na dim_dates.
-- ano_clima e mes_clima são inteiros derivados em stg_clima via CAST
-- das colunas text `ano` e `mes` presentes em load_stage.clima.
clima_agg as (
    select
        ano_clima,
        mes_clima,
        avg(temperatura_maxima) as clima_temp_max,
        sum(precipitacao_total) as clima_precipitacao
    from {{ ref('stg_clima') }}
    group by ano_clima, mes_clima
)

select
    d.sk_tempo,
    d.data_completa,
    d.ano,
    d.mes,
    d.dia,
    d.trimestre,
    d.nome_mes,
    d.dia_semana,
    d.is_dia_util,
    cast(c.clima_temp_max as numeric(5,2)) as clima_temp_max,
    cast(c.clima_precipitacao as numeric(5,2)) as clima_precipitacao
from dim_dates d
-- LEFT JOIN intencional: dias sem cobertura climática recebem NULL.
-- Atualmente apenas 2024 tem dados; anos fora desse intervalo → NULL esperado.
left join clima_agg c 
    on d.ano = c.ano_clima 
    and d.mes = c.mes_clima
