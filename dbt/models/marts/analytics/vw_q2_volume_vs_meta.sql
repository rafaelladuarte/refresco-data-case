{{ config(materialized='view') }}

WITH volume_realizado AS (
    SELECT 
        c.canal,
        SUM(f.qtd_caixas) AS volume_realizado_caixas,
        COUNT(DISTINCT f.sk_venda) AS total_vendas_realizadas,
        -- bonificações entram no volume, mas não na receita
        SUM(CASE WHEN f.entra_receita THEN f.receita_liquida ELSE 0 END) AS receita_realizada
    FROM {{ ref('fato_vendas') }} f
    JOIN {{ ref('dim_cliente') }} c ON f.sk_cliente = c.sk_cliente
    WHERE f.status_financeiro = 'PAGO'
      AND f.entra_volume           -- regra: seed regra_tipo_lancamento
      AND c.ativo = true
    GROUP BY 1
),
metas AS (
    SELECT 
        canal,
        volume_realizado_caixas *
        CASE 
            WHEN canal = 'Food Service' THEN 1.10
            WHEN canal = 'Conveniência' THEN 1.12
            WHEN canal = 'Supermercado' THEN 1.05
            ELSE 1.08
        END AS meta_volume_caixas
    FROM volume_realizado
)
SELECT 
    vr.canal,
    vr.volume_realizado_caixas,
    vr.receita_realizada,
    m.meta_volume_caixas,
    ROUND(
        (vr.volume_realizado_caixas * 100.0 / NULLIF(m.meta_volume_caixas, 0)),
        2
    ) AS percentual_atingimento,
    vr.total_vendas_realizadas
FROM volume_realizado vr
JOIN metas m ON vr.canal = m.canal