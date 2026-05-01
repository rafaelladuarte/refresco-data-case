-- =============================================================================
-- ANÁLISE DE PERFORMANCE - DASHBOARD
-- Gráficos sugeridos: Tabela com alerta, Barras horizontais
-- =============================================================================

-- -----------------------------------------------------------------------------
-- L1 - PRODUTOS COM MARGEM NEGATIVA OU CRÍTICA
-- Gráfico: Tabela com alerta (destaque em vermelho para margem negativa)
-- -----------------------------------------------------------------------------
SELECT 
    p.nome_produto,
    p.familia,
    p.marca,
    SUM(f.receita_liquida) AS receita_total,
    SUM(f.custo_total) AS custo_total,
    SUM(f.receita_liquida - f.custo_total) AS lucro_prejuizo,
    ROUND((SUM(f.receita_liquida - f.custo_total) * 100.0 / NULLIF(SUM(f.receita_liquida), 0))::NUMERIC, 2) AS margem_percentual,
    COUNT(DISTINCT f.sk_venda) AS total_vendas,
    SUM(f.qtd_caixas) AS volume_total
FROM gold.fato_vendas f
JOIN gold.dim_produto p ON f.sk_produto = p.sk_produto
WHERE f.status_financeiro = 'confirmado'
GROUP BY p.nome_produto, p.familia, p.marca
HAVING SUM(f.receita_liquida - f.custo_total) < 0
   OR (SUM(f.receita_liquida - f.custo_total) * 100.0 / NULLIF(SUM(f.receita_liquida), 0)) < 10
ORDER BY margem_percentual ASC;


-- -----------------------------------------------------------------------------
-- L2 - EFICIÊNCIA POR REPRESENTANTE
-- Gráfico: Barras horizontais + Tabela classificatória
-- -----------------------------------------------------------------------------
SELECT 
    r.nome_representante,
    r.canal_venda,
    COUNT(DISTINCT f.sk_venda) AS total_vendas,
    SUM(f.receita_liquida) AS receita_gerada,
    SUM(f.qtd_caixas) AS volume_vendido,
    COUNT(DISTINCT c.sk_cliente) AS clientes_atendidos,
    ROUND(AVG(f.receita_liquida)::NUMERIC, 2) AS ticket_medio_representante,
    ROUND((SUM(f.re