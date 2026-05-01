-- =============================================================================
-- VISÃO GERAL - DASHBOARD
-- Gráficos sugeridos: Cards/Metrics para KPIs e Linha para evolução temporal
-- =============================================================================

-- -----------------------------------------------------------------------------
-- K1 - INDICADORES GERAIS DO NEGÓCIO
-- Gráfico: Cards/Metrics (valores únicos destacados)
-- -----------------------------------------------------------------------------
SELECT 
    COUNT(DISTINCT sk_venda) AS total_vendas,
    SUM(qtd_caixas) AS volume_total_caixas,
    SUM(receita_liquida) AS receita_total,
    AVG(receita_liquida) AS ticket_medio,
    SUM(receita_liquida - custo_total) AS lucro_total,
    ROUND((SUM(receita_liquida - custo_total) * 100.0 / NULLIF(SUM(receita_liquida), 0))::NUMERIC, 2) AS margem_media
FROM gold.fato_vendas
WHERE status_financeiro = 'confirmado';


-- -----------------------------------------------------------------------------
-- K2 - EVOLUÇÃO TEMPORAL (ÚLTIMOS 12 MESES)
-- Gráfico: Linha (Trend Line) - múltiplas séries (receita, volume, margem)
-- -----------------------------------------------------------------------------
SELECT 
    t.ano,
    t.mes,
    TO_CHAR(DATE_TRUNC('month', t.data_referencia), 'YYYY-MM') AS ano_mes,
    COUNT(DISTINCT f.sk_venda) AS total_vendas,
    SUM(f.qtd_caixas) AS volume_caixas,
    SUM(f.receita_liquida) AS receita,
    SUM(f.receita_liquida - f.custo_total) AS lucro,
    ROUND((SUM(f.receita_liquida - f.custo_total) * 100.0 / NULLIF(SUM(f.receita_liquida), 0))::NUMERIC, 2) AS margem
FROM gold.fato_vendas f
JOIN gold.dim_tempo t ON f.sk_tempo = t.sk_tempo
WHERE f.status_financeiro = 'confirmado'
  AND t.data_referencia >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '12 months'
GROUP BY t.ano, t.mes, DATE_TRUNC('month', t.data_referencia)
ORDER BY t.ano, t.mes;


-- -----------------------------------------------------------------------------
-- K3 - ACUMULADO ANUAL VS META
-- Gráfico: Gauge (velocímetro) ou Progress Bar
-- -----------------------------------------------------------------------------
WITH acumulado_ano AS (
    SELECT 
        t.ano,
        SUM(f.receita_liquida) AS receita_acumulada,
        SUM(f.qtd_caixas) AS volume_acumulado,
        COUNT(DISTINCT f.sk_venda) AS vendas_acumuladas
    FROM gold.fato_vendas f
    JOIN gold.dim_tempo t ON f.sk_tempo = t.sk_tempo
    WHERE f.status_financeiro = 'confirmado'
      AND t.ano = EXTRACT(YEAR FROM CURRENT_DATE)
    GROUP BY t.ano
)
SELECT 
    ano,
    TO_CHAR(receita_acumulada, 'L999G999G999D99') AS receita_ytd,
    volume_acumulado,
    vendas_acumuladas,
    TO_CHAR(receita_acumulada * 1.15, 'L999G999G999D99') AS meta_ano_projetada,
    ROUND((receita_acumulada * 100.0 / (receita_acumulada * 1.15))::NUMERIC, 2) AS percentual_meta_atingido
FROM acumulado_ano;