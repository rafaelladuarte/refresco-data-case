/*
  A fato deve reconciliar com stg_vendas: mesma contagem e mesma soma de receita_liquida.
  Falha (retorna linhas) se um join multiplicar ou perder vendas (ex.: lançamento
  financeiro duplicado, como na Sprint 1).

  Como a fato é incremental, o teste vale para a carga completa: após um full-refresh
  ambas coincidem; em execuções incrementais, vendas novas entram nas duas.
*/
with staging as (
    select count(*) as qtd, coalesce(sum(receita_liquida), 0) as receita
    from {{ ref('stg_vendas') }}
),

fato as (
    select count(*) as qtd, coalesce(sum(receita_liquida), 0) as receita
    from {{ ref('fato_vendas') }}
)

select
    s.qtd as qtd_staging,
    f.qtd as qtd_fato,
    s.receita as receita_staging,
    f.receita as receita_fato
from staging s
cross join fato f
where s.qtd <> f.qtd
   or round(s.receita::numeric, 2) <> round(f.receita::numeric, 2)
