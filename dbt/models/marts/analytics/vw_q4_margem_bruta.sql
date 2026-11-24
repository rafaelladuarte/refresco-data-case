{{ config(materialized='view') }}

/*
  Margem bruta por produto, conforme a regra do seed regra_tipo_lancamento:
    - receita: somente vendas com entra_receita (VENDA)
    - custo:   vendas com entra_volume (VENDA + BONIFICACAO). Produto bonificado
               sai do estoque sem gerar receita, então reduz a margem.
*/
WITH base AS (
    SELECT
        p.sk_produto,
        p.nome_produto,
        CASE WHEN f.entra_receita THEN f.receita_liquida ELSE 0 END AS receita,
        COALESCE(f.custo_total, f.qtd_caixas * p.custo_unitario) AS custo
    FROM {{ ref('fato_vendas') }} f
    JOIN {{ ref('dim_produto') }} p ON f.sk_produto = p.sk_produto
    WHERE f.status_financeiro = 'PAGO'
      AND f.entra_volume
)

SELECT
    sk_produto,
    nome_produto,
    SUM(receita) AS receita_liquida_total,
    SUM(custo) AS custo_total,
    SUM(receita - custo) AS margem_bruta,
    ROUND(SUM(receita - custo) * 100.0 / NULLIF(SUM(receita), 0), 2) AS margem_bruta_percentual
FROM base
GROUP BY sk_produto, nome_produto
