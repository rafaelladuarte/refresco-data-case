/*
  Todo dia com venda deve ter clima diário na dim_tempo.
  Falha (retorna linhas) se a ingestão de clima não cobrir o período das vendas,
  o que deixaria análises de clima × vendas incompletas sem aviso.
*/
select distinct
    t.data_completa
from {{ ref('fato_vendas') }} f
join {{ ref('dim_tempo') }} t
    on f.sk_tempo = t.sk_tempo
where t.clima_temp_max is null
   or t.clima_precipitacao is null
