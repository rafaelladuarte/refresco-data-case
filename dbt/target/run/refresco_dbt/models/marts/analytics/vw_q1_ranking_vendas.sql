
  create view "gold"."gold"."vw_q1_ranking_vendas__dbt_tmp"
    
    
  as (
    

WITH vendas_regiao_produto AS (
    SELECT 
        p.sk_produto,
        p.nome_produto,
        p.familia,
        c.regiao,
        SUM(f.receita_liquida) AS total_receita_liquida,
        COUNT(DISTINCT f.sk_venda) AS total_vendas,
        SUM(f.qtd_caixas) AS total_caixas
    FROM "gold"."gold"."fato_vendas" f
    JOIN "gold"."gold"."dim_produto" p ON f.sk_produto = p.sk_produto
    JOIN "gold"."gold"."dim_cliente" c ON f.sk_cliente = c.sk_cliente
    WHERE f.status_financeiro = 'PAGO'
      AND c.ativo = true
    GROUP BY p.sk_produto, p.nome_produto, p.familia, c.regiao
),
ranking AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY regiao 
            ORDER BY total_receita_liquida DESC
        ) AS ranking
    FROM vendas_regiao_produto
)
SELECT *
FROM ranking
WHERE ranking <= 10
  );