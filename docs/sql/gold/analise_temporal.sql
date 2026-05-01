-- =============================================================================
-- ANÁLISE TEMPORAL - DASHBOARD
-- Gráficos sugeridos: Barras, Linha, Indicador de Variação
-- =============================================================================

-- -----------------------------------------------------------------------------
-- T1 - SAZONALIDADE POR DIA DA SEMANA
-- Gráfico: Barras ou Linha (por dia da semana)
-- -----------------------------------------------------------------------------
SELECT 
    t.dia_semana,
    COUNT(DISTINCT f.sk_venda) AS qtde_vendas,
    SUM(f.receita_liquida) AS receita_total,
    SUM(f.qtd_caixas) AS volume_total,
    ROUND(AVG(f.receita_liquida)::NUMERIC, 2) AS ticket_medio,
    COUNT(DISTINCT c.sk_cliente) AS clientes_unicos
FROM gold.fato_vendas f
JOIN gold.dim_tempo t ON f.sk_tempo = t.sk_tempo
JOIN gold.dim_cliente c ON f.sk_cliente = c.sk_cliente
WHERE f.status_financeiro = 'confirmado'
GROUP BY t.dia_semana
ORDER BY 
    CASE t.dia_semana
        WHEN 'Sunday' THEN 1
        WHEN 'Monday' THEN 2
        WHEN 'Tuesday' THEN 3
        WHEN 'Wednesday' THEN 4
        WHEN 'Thursday' THEN 5
        WHEN 'Friday' THEN 6
        WHEN 'Saturday' THEN 7
    END;


-- -----------------------------------------------------------------------------
-- T2 - COMPARATIVO MÊS ATUAL VS MÊS ANTERIOR
-- Gráfico: Indicador de Variação (Up/Down) + Cards comparativos
-- -----------------------------------------------------------------------------
WITH vendas_mes AS (
    SELECT 
        DATE_TRUNC('month', t.data_referencia) AS mes,
        SUM(f.receita_liquida) AS receita,
        SUM(f.qtd_caixas) AS volume,
        COUNT(DISTINCT f.sk_venda) AS vendas,
        COUNT(DISTINCT c.sk_cliente) AS clientes_ativos,
        ROUND((SUM(f.receita_liquida - f.custo_total) * 100.0 / NULLIF(SUM(f.receita_liquida), 0))::NUMERIC, 2) AS margem
    FROM gold.fato_vendas f
    JOIN gold.dim_tempo t ON f.sk_tempo = t.sk_tempo
    JOIN gold.dim_cliente c ON f.sk_cliente = c.sk_cliente
    WHERE f.status_financeiro = 'confirmado'
      AND t.data_referencia >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '2 months'
    GROUP BY DATE_TRUNC('month', t.data_referencia)
)
SELECT 
    TO_CHAR(mes, 'YYYY-MM') AS mes_referencia,
    TO_CHAR(receita, 'L999G999G999D99') AS receita,
    volume,
    vendas,
    clientes_ativos,
    margem,
    LAG(receita) OVER (ORDER BY mes) AS receita_mes_anterior,
    LAG(volume) OVER (ORDER BY mes) AS volume_mes_anterior,
    ROUND(((receita - LAG(receita) OVER (ORDER BY mes)) * 100.0 / NULLIF(LAG(receita) OVER (ORDER BY mes), 0))::NUMERIC, 2) AS variacao_receita_pct,
    ROUND(((volume - LAG(volume) OVER (ORDER BY mes)) * 100.0 / NULLIF(LAG(volume) OVER (ORDER BY mes), 0))::NUMERIC, 2) AS variacao_volume_pct
FROM vendas_mes
ORDER BY mes DESC;


-- -----------------------------------------------------------------------------
-- T3 - ANÁLISE DE TENDÊNCIA DE VOLUME POR PRODUTO (MÉDIA MÓVEL 7 DIAS)
-- Gráfico: Linha com média móvel (produtos específicos)
-- -----------------------------------------------------------------------------
WITH vendas_diarias_produto AS (
    SELECT 
        p.nome_produto,
        p.sk_produto,
        t.data_referencia,
        SUM(f.qtd_caixas) AS volume_diario_caixas,
        SUM(f.receita_liquida) AS receita_diaria,
        COUNT(DISTINCT f.sk_venda) AS qtde_vendas_dia
    FROM gold.fato_vendas f
    JOIN gold.dim_produto p ON f.sk_produto = p.sk_produto
    JOIN gold.dim_tempo t ON f.sk_tempo = t.sk_tempo
    WHERE f.status_financeiro = 'confirmado'
      AND t.data_referencia >= CURRENT_DATE - INTERVAL '90 days'
      AND p.nome_produto IN ('PRODUTO_A', 'PRODUTO_B') -- Ajuste para produtos específicos
    GROUP BY p.nome_produto, p.sk_produto, t.data_referencia
),
analise_tendencia AS (
    SELECT 
        nome_produto,
        data_referencia,
        volume_diario_caixas,
        LAG(volume_diario_caixas, 1) OVER (
            PARTITION BY nome_produto 
            ORDER BY data_referencia
        ) AS volume_dia_anterior,
        AVG(volume_diario_caixas) OVER (
            PARTITION BY nome_produto 
            ORDER BY data_referencia 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS media_movel_7dias,
        AVG(volume_diario_caixas) OVER (
            PARTITION BY nome_produto 
            ORDER BY data_referencia 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS media_movel_30dias,
        receita_diaria,
        qtde_vendas_dia
    FROM vendas_diarias_produto
)
SELECT 
    nome_produto,
    TO_CHAR(data_referencia, 'DD/MM/YYYY') AS data,
    volume_diario_caixas AS volume_atual,
    COALESCE(volume_dia_anterior, 0) AS volume_dia_anterior,
    CASE 
        WHEN volume_dia_anterior > 0 
        THEN ROUND(((volume_diario_caixas - volume_dia_anterior) * 100.0 / volume_dia_anterior)::NUMERIC, 2)
        ELSE NULL 
    END AS variacao_dia_anterior_pct,
    ROUND(media_movel_7dias::NUMERIC, 2) AS media_movel_7dias,
    ROUND(media_movel_30dias::NUMERIC, 2) AS media_movel_30dias,
    CASE 
        WHEN volume_diario_caixas > media_movel_7dias AND media_movel_7dias > media_movel_30dias
        THEN 'TENDENCIA_ALTA'
        WHEN volume_diario_caixas < media_movel_7dias AND media_movel_7dias < media_movel_30dias
        THEN 'TENDENCIA_BAIXA'
        ELSE 'ESTAVEL_VOLATILIDADE'
    END AS sinal_tendencia,
    TO_CHAR(receita_diaria, 'L999G999D99') AS receita_diaria
FROM analise_tendencia
WHERE volume_diario_caixas > 0
ORDER BY nome_produto, data_referencia DESC;


-- -----------------------------------------------------------------------------
-- T4 - RANKING DE VENDAS POR PRODUTO E REGIÃO (TOP 10 POR REGIÃO)
-- Gráfico: Barras agrupadas (produtos por região) ou Tabela heatmap
-- -----------------------------------------------------------------------------
WITH vendas_regiao_produto AS (
    SELECT 
        p.nome_produto,
        p.familia,
        c.regiao,
        SUM(f.receita_liquida) AS total_receita_liquida,
        COUNT(DISTINCT f.sk_venda) AS total_vendas,
        SUM(f.qtd_caixas) AS total_caixas
    FROM gold.fato_vendas f
    JOIN gold.dim_produto p ON f.sk_produto = p.sk_produto
    JOIN gold.dim_cliente c ON f.sk_cliente = c.sk_cliente
    WHERE f.status_financeiro = 'confirmado'
      AND c.ativo = true
    GROUP BY p.nome_produto, p.familia, c.regiao
),
ranking_regiao AS (
    SELECT 
        regiao,
        nome_produto,
        familia,
        total_receita_liquida,
        total_vendas,
        total_caixas,
        ROW_NUMBER() OVER (PARTITION BY regiao ORDER BY total_receita_liquida DESC) AS rank_produto
    FROM vendas_regiao_produto
)
SELECT 
    regiao,
    rank_produto AS ranking,
    nome_produto,
    familia,
    TO_CHAR(total_receita_liquida, 'L999G999G999D99') AS receita_liquida,
    total_vendas,
    total_caixas
FROM ranking_regiao
WHERE rank_produto <= 10
ORDER BY regiao, rank_produto;