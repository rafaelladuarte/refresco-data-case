-- =============================================================================
-- ANÁLISE POR CLIENTE - DASHBOARD
-- Gráficos sugeridos: Barras, Donut, Matriz Scatter, Tabela segmentada
-- =============================================================================

-- -----------------------------------------------------------------------------
-- C1 - TOP CLIENTES POR RECEITA
-- Gráfico: Barras (Top clientes) + Tabela com detalhes
-- -----------------------------------------------------------------------------
SELECT 
    c.nome_cliente,
    c.canal,
    c.regiao,
    c.uf,
    SUM(f.receita_liquida) AS receita_total,
    SUM(f.qtd_caixas) AS volume_total,
    COUNT(DISTINCT f.sk_venda) AS qtde_compras,
    ROUND(AVG(f.receita_liquida)::NUMERIC, 2) AS ticket_medio,
    MAX(t.data_referencia) AS ultima_compra
FROM gold.fato_vendas f
JOIN gold.dim_cliente c ON f.sk_cliente = c.sk_cliente
JOIN gold.dim_tempo t ON f.sk_tempo = t.sk_tempo
WHERE f.status_financeiro = 'confirmado'
  AND c.ativo = true
GROUP BY c.nome_cliente, c.canal, c.regiao, c.uf
ORDER BY receita_total DESC
LIMIT 20;


-- -----------------------------------------------------------------------------
-- C2 - DISTRIBUIÇÃO POR CANAL DE VENDA
-- Gráfico: Donut (participação por canal) + Barras comparativas
-- -----------------------------------------------------------------------------
SELECT 
    c.canal,
    COUNT(DISTINCT c.sk_cliente) AS qtde_clientes,
    SUM(f.receita_liquida) AS receita_total,
    SUM(f.qtd_caixas) AS volume_total,
    COUNT(DISTINCT f.sk_venda) AS qtde_vendas,
    ROUND(AVG(f.receita_liquida)::NUMERIC, 2) AS ticket_medio,
    ROUND((SUM(f.receita_liquida - f.custo_total) * 100.0 / NULLIF(SUM(f.receita_liquida), 0))::NUMERIC, 2) AS margem_media
FROM gold.fato_vendas f
JOIN gold.dim_cliente c ON f.sk_cliente = c.sk_cliente
WHERE f.status_financeiro = 'confirmado'
  AND c.ativo = true
GROUP BY c.canal
ORDER BY receita_total DESC;


-- -----------------------------------------------------------------------------
-- C3 - VOLUME REALIZADO POR CANAL (PERCENTUAL DE META)
-- Gráfico: Barras com linha de meta (comparativo)
-- -----------------------------------------------------------------------------
WITH metas_canal AS (
    VALUES 
        ('DISTRIBUIDOR', 30000),
        ('ATACADO', 25000),
        ('VAREJO', 15000),
        ('E-COMMERCE', 8000),
        ('FOOD SERVICE', 12000)
),
volume_realizado AS (
    SELECT 
        c.canal,
        SUM(f.qtd_caixas) AS volume_realizado_caixas,
        COUNT(DISTINCT f.sk_venda) AS total_vendas_realizadas,
        SUM(f.receita_liquida) AS receita_realizada
    FROM gold.fato_vendas f
    JOIN gold.dim_cliente c ON f.sk_cliente = c.sk_cliente
    WHERE f.status_financeiro = 'confirmado'
      AND c.ativo = true
      AND f.data_inclusao >= CURRENT_DATE - INTERVAL '12 months'
    GROUP BY c.canal
)
SELECT 
    vr.canal,
    vr.volume_realizado_caixas,
    vr.receita_realizada,
    mc.column2 AS meta_volume_caixas,
    ROUND(
        (vr.volume_realizado_caixas * 100.0 / NULLIF(mc.column2, 0))::NUMERIC, 
        2
    ) AS percentual_atingimento,
    vr.total_vendas_realizadas,
    CASE 
        WHEN (vr.volume_realizado_caixas * 100.0 / NULLIF(mc.column2, 0)) >= 100 
        THEN 'META ATINGIDA'
        WHEN (vr.volume_realizado_caixas * 100.0 / NULLIF(mc.column2, 0)) >= 80 
        THEN 'PROXIMO DA META'
        ELSE 'ABAIXO DA META'
    END AS status_meta
FROM volume_realizado vr
LEFT JOIN metas_canal mc ON vr.canal = mc.column1
ORDER BY percentual_atingimento DESC;


-- -----------------------------------------------------------------------------
-- C4 - ANÁLISE DE RECÊNCIA, FREQUÊNCIA E VALOR (RFV)
-- Gráfico: Matriz RFV (Scatter Plot) + Tabela segmentada
-- -----------------------------------------------------------------------------
WITH rfv_cliente AS (
    SELECT 
        c.sk_cliente,
        c.nome_cliente,
        c.canal,
        c.regiao,
        MAX(t.data_referencia) AS data_ultima_compra,
        COUNT(DISTINCT f.sk_venda) AS frequencia,
        SUM(f.receita_liquida) AS valor_total,
        CURRENT_DATE - MAX(t.data_referencia) AS dias_sem_compra
    FROM gold.fato_vendas f
    JOIN gold.dim_cliente c ON f.sk_cliente = c.sk_cliente
    JOIN gold.dim_tempo t ON f.sk_tempo = t.sk_tempo
    WHERE f.status_financeiro = 'confirmado'
    GROUP BY c.sk_cliente, c.nome_cliente, c.canal, c.regiao
),
rfv_score AS (
    SELECT 
        *,
        NTILE(5) OVER (ORDER BY dias_sem_compra ASC) AS recencia_score,
        NTILE(5) OVER (ORDER BY frequencia DESC) AS frequencia_score,
        NTILE(5) OVER (ORDER BY valor_total DESC) AS valor_score,
        (NTILE(5) OVER (ORDER BY dias_sem_compra ASC) + 
         NTILE(5) OVER (ORDER BY frequencia DESC) + 
         NTILE(5) OVER (ORDER BY valor_total DESC)) AS rfv_total
    FROM rfv_cliente
)
SELECT 
    nome_cliente,
    canal,
    regiao,
    dias_sem_compra,
    frequencia,
    TO_CHAR(valor_total, 'L999G999D99') AS valor_total,
    CASE 
        WHEN rfv_total >= 13 THEN 'CLIENTE VIP'
        WHEN rfv_total >= 10 THEN 'CLIENTE FREQUENTE'
        WHEN rfv_total >= 7 THEN 'CLIENTE REGULAR'
        ELSE 'CLIENTE EM RISCO'
    END AS segmento_rfv
FROM rfv_score
ORDER BY rfv_total DESC, valor_total DESC
LIMIT 50;