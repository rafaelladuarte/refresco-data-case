-- =============================================================================
-- ANÁLISE POR PRODUTO - DASHBOARD
-- Gráficos sugeridos: Barras horizontal, Treemap, Bolhas (Bubble Chart)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- P1 - TOP 10 PRODUTOS POR RECEITA
-- Gráfico: Barras horizontal (ranking de produtos)
-- -----------------------------------------------------------------------------
SELECT 
    p.nome_produto,
    p.familia,
    p.marca,
    SUM(f.receita_liquida) AS receita_total,
    SUM(f.qtd_caixas) AS volume_total,
    COUNT(DISTINCT f.sk_venda) AS qtde_vendas,
    ROUND(SUM(f.receita_liquida - f.custo_total)::NUMERIC, 2) AS lucro_total,
    ROUND((SUM(f.receita_liquida - f.custo_total) * 100.0 / NULLIF(SUM(f.receita_liquida), 0))::NUMERIC, 2) AS margem
FROM gold.fato_vendas f
JOIN gold.dim_produto p ON f.sk_produto = p.sk_produto
WHERE f.status_financeiro = 'confirmado'
GROUP BY p.nome_produto, p.familia, p.marca
ORDER BY receita_total DESC
LIMIT 10;


-- -----------------------------------------------------------------------------
-- P2 - PERFORMANCE POR FAMÍLIA DE PRODUTO
-- Gráfico: Treemap ou Pizza (participação por família)
-- -----------------------------------------------------------------------------
SELECT 
    p.familia,
    SUM(f.receita_liquida) AS receita_total,
    SUM(f.qtd_caixas) AS volume_total,
    COUNT(DISTINCT p.sk_produto) AS qtde_produtos,
    ROUND(AVG(f.receita_liquida - f.custo_total)::NUMERIC, 2) AS lucro_medio,
    ROUND((SUM(f.receita_liquida - f.custo_total) * 100.0 / NULLIF(SUM(f.receita_liquida), 0))::NUMERIC, 2) AS margem_media,
    RANK() OVER (ORDER BY SUM(f.receita_liquida) DESC) AS ranking_receita
FROM gold.fato_vendas f
JOIN gold.dim_produto p ON f.sk_produto = p.sk_produto
WHERE f.status_financeiro = 'confirmado'
GROUP BY p.familia
ORDER BY receita_total DESC;


-- -----------------------------------------------------------------------------
-- P3 - MATRIZ DE RENTABILIDADE (MARGEM VS VOLUME)
-- Gráfico: Bolhas (Bubble Chart) - Volume no eixo X, Margem no eixo Y, tamanho = Receita
-- -----------------------------------------------------------------------------
SELECT 
    p.nome_produto,
    p.familia,
    SUM(f.qtd_caixas) AS volume_total,
    SUM(f.receita_liquida) AS receita,
    ROUND((SUM(f.receita_liquida - f.custo_total) * 100.0 / NULLIF(SUM(f.receita_liquida), 0))::NUMERIC, 2) AS margem_percentual,
    CASE 
        WHEN SUM(f.qtd_caixas) > PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY SUM(f.qtd_caixas)) OVER() 
             AND (SUM(f.receita_liquida - f.custo_total) * 100.0 / NULLIF(SUM(f.receita_liquida), 0)) > PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY (SUM(f.receita_liquida - f.custo_total) * 100.0 / NULLIF(SUM(f.receita_liquida), 0))) OVER()
        THEN 'ESTRELA (Alto Volume + Alta Margem)'
        WHEN SUM(f.qtd_caixas) > PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY SUM(f.qtd_caixas)) OVER() 
        THEN 'VOLUME (Alto Volume)'
        WHEN (SUM(f.receita_liquida - f.custo_total) * 100.0 / NULLIF(SUM(f.receita_liquida), 0)) > PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY (SUM(f.receita_liquida - f.custo_total) * 100.0 / NULLIF(SUM(f.receita_liquida), 0))) OVER()
        THEN 'RENTAVEL (Alta Margem)'
        ELSE 'OPORTUNIDADE'
    END AS classificacao
FROM gold.fato_vendas f
JOIN gold.dim_produto p ON f.sk_produto = p.sk_produto
WHERE f.status_financeiro = 'confirmado'
GROUP BY p.nome_produto, p.familia
ORDER BY volume_total DESC
LIMIT 100;


-- -----------------------------------------------------------------------------
-- P4 - MARGEM BRUTA POR SKU (DETALHADO)
-- Gráfico: Tabela com ordenação e cores por performance
-- -----------------------------------------------------------------------------
WITH margem_sku AS (
    SELECT 
        p.sk_produto,
        p.id_produto,
        p.nome_produto,
        p.familia,
        p.marca,
        p.embalagem,
        p.volume_ml,
        p.preco_tabela,
        p.custo_unitario,
        COUNT(DISTINCT f.sk_venda) AS total_vendas,
        SUM(f.qtd_caixas) AS volume_total_caixas,
        SUM(f.receita_liquida) AS receita_liquida_total,
        SUM(COALESCE(f.custo_total, f.qtd_caixas * p.custo_unitario, 0)) AS custo_total_apurado,
        SUM(f.receita_liquida - COALESCE(f.custo_total, f.qtd_caixas * p.custo_unitario, 0)) AS margem_bruta_total
    FROM gold.fato_vendas f
    JOIN gold.dim_produto p ON f.sk_produto = p.sk_produto
    WHERE f.status_financeiro = 'confirmado'
      AND f.receita_liquida > 0
    GROUP BY p.sk_produto, p.id_produto, p.nome_produto, p.familia, 
             p.marca, p.embalagem, p.volume_ml, p.preco_tabela, p.custo_unitario
    HAVING SUM(f.receita_liquida) > 1000
)
SELECT 
    sk_produto AS codigo_sku,
    id_produto,
    nome_produto,
    familia,
    marca,
    embalagem,
    volume_ml || 'ml' AS volume,
    total_vendas,
    volume_total_caixas,
    TO_CHAR(receita_liquida_total, 'L999G999G999D99') AS receita_liquida,
    TO_CHAR(margem_bruta_total, 'L999G999G999D99') AS margem_bruta_valor,
    ROUND(
        (margem_bruta_total * 100.0 / NULLIF(receita_liquida_total, 0))::NUMERIC, 
        2
    ) AS margem_bruta_percentual,
    CASE 
        WHEN (margem_bruta_total * 100.0 / NULLIF(receita_liquida_total, 0)) >= 50 
        THEN 'ALTA RENTABILIDADE'
        WHEN (margem_bruta_total * 100.0 / NULLIF(receita_liquida_total, 0)) >= 30 
        THEN 'MARGEM MEDIA'
        WHEN (margem_bruta_total * 100.0 / NULLIF(receita_liquida_total, 0)) >= 15 
        THEN 'BAIXA MARGEM'
        ELSE 'MARGEM CRITICA'
    END AS classificacao_margem
FROM margem_sku
ORDER BY margem_bruta_percentual DESC, receita_liquida_total DESC
LIMIT 50;