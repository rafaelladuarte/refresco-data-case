{{ config(materialized='view') }}

WITH vendas_diarias AS (
    SELECT 
        p.sk_produto,
        p.nome_produto,
        t.data_completa,
        SUM(f.qtd_caixas) AS volume_diario_caixas
    FROM {{ ref('fato_vendas') }} f
    JOIN {{ ref('dim_produto') }} p ON f.sk_produto = p.sk_produto
    JOIN {{ ref('dim_tempo') }} t ON f.sk_tempo = t.sk_tempo
    WHERE f.status_financeiro = 'PAGO'
    GROUP BY p.sk_produto, p.nome_produto, t.data_completa
),
analise AS (
    SELECT 
        *,
        LAG(volume_diario_caixas) OVER (
            PARTITION BY sk_produto
            ORDER BY data_completa
        ) AS volume_dia_anterior,
        AVG(volume_diario_caixas) OVER (
            PARTITION BY sk_produto
            ORDER BY data_completa
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS media_movel_7dias
    FROM vendas_diarias
)
SELECT 
    sk_produto,
    nome_produto,
    data_completa,
    volume_diario_caixas,
    volume_dia_anterior,
    ROUND(media_movel_7dias, 2) AS media_movel_7dias
FROM analise
WHERE volume_diario_caixas > 0