{{ config(materialized='view') }}

/*
  Volume realizado vs. meta por canal.

  - Realizado: vendas PAGO com entra_volume (seed regra_tipo_lancamento), clientes ativos.
    Receita só das vendas com entra_receita (bonificação entra no volume, não na receita).
  - Meta: seed metas_canal_mes (valores FICTÍCIOS, independentes do realizado).
  - O cruzamento é feito no grão canal × ano × mês (FULL OUTER JOIN): mês com meta e
    sem venda conta como meta não atingida; canal sem meta (ex.: 'Não informado')
    aparece com meta e percentual NULL.
  - O contrato de colunas é mantido para o dashboard (streamlit/main.py).
*/
WITH realizado_mes AS (
    SELECT
        c.canal,
        t.ano,
        t.mes,
        SUM(f.qtd_caixas) AS volume_caixas,
        SUM(CASE WHEN f.entra_receita THEN f.receita_liquida ELSE 0 END) AS receita,
        COUNT(DISTINCT f.sk_venda) AS total_vendas
    FROM {{ ref('fato_vendas') }} f
    JOIN {{ ref('dim_cliente') }} c ON f.sk_cliente = c.sk_cliente
    JOIN {{ ref('dim_tempo') }} t ON f.sk_tempo = t.sk_tempo
    WHERE f.status_financeiro = 'PAGO'
      AND f.entra_volume
      AND c.ativo = true
    GROUP BY c.canal, t.ano, t.mes
),

metas_mes AS (
    SELECT canal, ano, mes, meta_volume_caixas
    FROM {{ ref('metas_canal_mes') }}
),

canal_mes AS (
    SELECT
        COALESCE(r.canal, m.canal) AS canal,
        COALESCE(r.volume_caixas, 0) AS volume_caixas,
        COALESCE(r.receita, 0) AS receita,
        COALESCE(r.total_vendas, 0) AS total_vendas,
        m.meta_volume_caixas
    FROM realizado_mes r
    FULL OUTER JOIN metas_mes m
        ON r.canal = m.canal
        AND r.ano = m.ano
        AND r.mes = m.mes
)

SELECT
    canal,
    SUM(volume_caixas) AS volume_realizado_caixas,
    SUM(receita) AS receita_realizada,
    SUM(meta_volume_caixas) AS meta_volume_caixas,
    ROUND(SUM(volume_caixas) * 100.0 / NULLIF(SUM(meta_volume_caixas), 0), 2) AS percentual_atingimento,
    SUM(total_vendas) AS total_vendas_realizadas
FROM canal_mes
GROUP BY canal
