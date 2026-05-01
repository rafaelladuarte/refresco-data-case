-- =============================================================================
-- ANÁLISE GEOGRÁFICA - DASHBOARD
-- Gráficos sugeridos: Mapa coroplético, Barras por região
-- =============================================================================

-- -----------------------------------------------------------------------------
-- G1 - VENDAS POR REGIÃO E UF
-- Gráfico: Mapa coroplético (Brasil por UF) + Barras por região
-- -----------------------------------------------------------------------------
SELECT 
    c.regiao,
    c.uf,
    COUNT(DISTINCT c.sk_cliente) AS qtde_clientes,
    SUM(f.receita_liquida) AS receita_total,
    SUM(f.qtd_caixas) AS volume_total,
    COUNT(DISTINCT f.sk_venda) AS qtde_vendas,
    ROUND(AVG(f.receita_liquida)::NUMERIC, 2) AS ticket_medio,
    ROUND((SUM(f.receita_liquida - f.custo_total) * 100.0 / NULLIF(SUM(f.receita_liquida), 0))::NUMERIC, 2) AS margem_media,
    RANK() OVER (ORDER BY SUM(f.receita_liquida) DESC) AS ranking_receita
FROM gold.fato_vendas f
JOIN gold.dim_cliente c ON f.sk_cliente = c.sk_cliente
WHERE f.status_financeiro = 'confirmado'
  AND c.ativo = true
GROUP BY c.regiao, c.uf
ORDER BY receita_total DESC;


-- -----------------------------------------------------------------------------
-- G2 - TOP CIDADES COM MAIOR CRESCIMENTO (ÚLTIMOS 6 MESES)
-- Gráfico: Barras com indicador de crescimento (positivo/negativo)
-- -----------------------------------------------------------------------------
WITH vendas_cidade AS (
    SELECT 
        c.cidade,
        c.uf,
        DATE_TRUNC('month', t.data_referencia) AS mes_referencia,
        SUM(f.receita_liquida) AS receita_mes
    FROM gold.fato_vendas f
    JOIN gold.dim_cliente c ON f.sk_cliente = c.sk_cliente
    JOIN gold.dim_tempo t ON f.sk_tempo = t.sk_tempo
    WHERE f.status_financeiro = 'confirmado'
      AND t.data_referencia >= CURRENT_DATE - INTERVAL '6 months'
    GROUP BY c.cidade, c.uf, DATE_TRUNC('month', t.data_referencia)
),
crescimento_cidade AS (
    SELECT 
        cidade,
        uf,
        MAX(CASE WHEN mes_referencia = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') THEN receita_mes ELSE 0 END) AS receita_ultimo_mes,
        AVG(CASE WHEN mes_referencia < DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') THEN receita_mes ELSE NULL END) AS media_meses_anteriores
    FROM vendas_cidade
    GROUP BY cidade, uf
    HAVING SUM(receita_mes) > 10000
)
SELECT 
    cidade,
    uf,
    TO_CHAR(receita_ultimo_mes, 'L999G999D99') AS receita_ultimo_mes,
    TO_CHAR(media_meses_anteriores, 'L999G999D99') AS media_anterior,
    ROUND(((receita_ultimo_mes - media_meses_anteriores) * 100.0 / NULLIF(media_meses_anteriores, 0))::NUMERIC, 2) AS crescimento_pct,
    CASE 
        WHEN receita_ultimo_mes > media_meses_anteriores THEN 'CRESCIMENTO'
        WHEN receita_ultimo_mes < media_meses_anteriores THEN 'DECLINIO'
        ELSE 'ESTAVEL'
    END AS tendencia
FROM crescimento_cidade
WHERE media_meses_anteriores > 0
ORDER BY crescimento_pct DESC
LIMIT 15;


-- -----------------------------------------------------------------------------
-- G3 - PERFORMANCE DE VENDAS POR REGIÃO (RANKING + PARTICIPAÇÃO)
-- Gráfico: Barras empilhadas (participação por UF dentro da região)
-- -----------------------------------------------------------------------------
SELECT 
    c.regiao,
    c.uf,
    SUM(f.receita_liquida) AS receita_total,
    ROUND(100.0 * SUM(f.receita_liquida) / SUM(SUM(f.receita_liquida)) OVER (PARTITION BY c.regiao), 2) AS participacao_regiao,
    ROUND(100.0 * SUM(f.receita_liquida) / SUM(SUM(f.receita_liquida)) OVER (), 2) AS participacao_global,
    SUM(f.qtd_caixas) AS volume_total
FROM gold.fato_vendas f
JOIN gold.dim_cliente c ON f.sk_cliente = c.sk_cliente
WHERE f.status_financeiro = 'confirmado'
  AND c.ativo = true
GROUP BY c.regiao, c.uf
ORDER BY c.regiao, receita_total DESC;