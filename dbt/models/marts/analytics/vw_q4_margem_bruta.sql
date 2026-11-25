{{ config(materialized='view') }}

/*
  Margem bruta por produto, conforme a regra do seed regra_tipo_lancamento:
    - receita: somente vendas com entra_receita (VENDA)
    - custo:   vendas com entra_volume (VENDA + BONIFICACAO). Produto bonificado
               sai do estoque sem gerar receita, então reduz a margem.

  Produto sem custo na origem (custo_disponivel = false, DQ-09) aparece com a receita,
  mas custo e margem ficam NULL: custo zero produziria uma margem de 100% falsa.
*/
WITH base AS (
    SELECT
        p.sk_produto,
        p.nome_produto,
        p.custo_disponivel,
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
    custo_disponivel,
    SUM(receita) AS receita_liquida_total,
    CASE WHEN custo_disponivel THEN SUM(custo) END AS custo_total,
    CASE WHEN custo_disponivel THEN SUM(receita - custo) END AS margem_bruta,
    CASE WHEN custo_disponivel
         THEN ROUND(SUM(receita - custo) * 100.0 / NULLIF(SUM(receita), 0), 2)
    END AS margem_bruta_percentual
FROM base
GROUP BY sk_produto, nome_produto, custo_disponivel
