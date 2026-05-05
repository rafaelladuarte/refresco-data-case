{{ config(materialized='view') }}

SELECT 
    p.sk_produto,
    p.nome_produto,
    SUM(f.receita_liquida) AS receita_liquida_total,
    SUM(COALESCE(f.custo_total, f.qtd_caixas * p.custo_unitario)) AS custo_total,
    SUM(
        f.receita_liquida - 
        COALESCE(f.custo_total, f.qtd_caixas * p.custo_unitario)
    ) AS margem_bruta,
    ROUND(
        (
            SUM(
                f.receita_liquida - 
                COALESCE(f.custo_total, f.qtd_caixas * p.custo_unitario)
            ) * 100.0
        ) / NULLIF(SUM(f.receita_liquida), 0),
        2
    ) AS margem_bruta_percentual
FROM {{ ref('fato_vendas') }} f
JOIN {{ ref('dim_produto') }} p ON f.sk_produto = p.sk_produto
WHERE f.status_financeiro = 'PAGO'
  AND f.receita_liquida > 0
GROUP BY p.sk_produto, p.nome_produto
