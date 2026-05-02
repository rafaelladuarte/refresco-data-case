--Q1: Ranking de vendas por produto e região

CREATE OR REPLACE VIEW gold.vw_Q1_ranking_produto_regiao AS
WITH vendas_regiao_produto AS (
    SELECT 
        p.sk_produto,
        p.nome_produto,
        p.familia,
        c.regiao,
        SUM(f.receita_liquida) AS total_receita_liquida,
        COUNT(DISTINCT f.sk_venda) AS total_vendas,
        SUM(f.qtd_caixas) AS total_caixas
    FROM gold.fato_vendas f
    JOIN gold.dim_produto p ON f.sk_produto = p.sk_produto
    JOIN gold.dim_cliente c ON f.sk_cliente = c.sk_cliente
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
WHERE ranking <= 10;


--Q2: Volume realizado vs meta por canal 

CREATE OR REPLACE VIEW gold.vw_Q2_meta_vs_realizado_canal AS
WITH volume_realizado AS (
    SELECT 
        CASE 
            WHEN c.canal = 'nan' THEN 'Outros'
            ELSE c.canal
        END AS canal,
        SUM(f.qtd_caixas) AS volume_realizado_caixas,
        COUNT(DISTINCT f.sk_venda) AS total_vendas_realizadas,
        SUM(f.receita_liquida) AS receita_realizada
    FROM gold.fato_vendas f
    JOIN gold.dim_cliente c ON f.sk_cliente = c.sk_cliente
    WHERE f.status_financeiro = 'PAGO'
      AND c.ativo = true
      AND c.canal IS NOT NULL
    GROUP BY 1
),
metas AS (
    SELECT 
        canal,
        volume_realizado_caixas *
        CASE 
            WHEN canal = 'Food Service' THEN 1.10
            WHEN canal = 'Conveniência' THEN 1.12
            WHEN canal = 'Supermercado' THEN 1.05
            ELSE 1.08
        END AS meta_volume_caixas
    FROM volume_realizado
)
SELECT 
    vr.canal,
    vr.volume_realizado_caixas,
    vr.receita_realizada,
    m.meta_volume_caixas,
    ROUND(
        (vr.volume_realizado_caixas * 100.0 / NULLIF(m.meta_volume_caixas, 0)),
        2
    ) AS percentual_atingimento,
    vr.total_vendas_realizadas
FROM volume_realizado vr
JOIN metas m ON vr.canal = m.canal;

--Q3: Análise de tendência de volume por produto

CREATE OR REPLACE VIEW gold.vw_Q3_tendencia_volume_produto AS
WITH vendas_diarias AS (
    SELECT 
        p.sk_produto,
        p.nome_produto,
        t.data_completa,
        SUM(f.qtd_caixas) AS volume_diario_caixas
    FROM gold.fato_vendas f
    JOIN gold.dim_produto p ON f.sk_produto = p.sk_produto
    JOIN gold.dim_tempo t ON f.sk_tempo = t.sk_tempo
    WHERE f.status_financeiro = 'PAGO'
    GROUP BY p.sk_produto, p.nome_produto, t.data_completa
),
analise AS (
    SELECT 
        *,
        LAG(volume_diario_caixas) OVER (
            PARTITION BY sk_produto
            ORDER BY data_completa
        ) AS volume_dia_anterior,
        AVG(volume_diario_caixas) OVER (
            PARTITION BY sk_produto
            ORDER BY data_completa
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS media_movel_7dias
    FROM vendas_diarias
)
SELECT 
    sk_produto,
    nome_produto,
    data_completa,
    volume_diario_caixas,
    volume_dia_anterior,
    ROUND(media_movel_7dias, 2) AS media_movel_7dias
FROM analise
WHERE volume_diario_caixas > 0;

--Q4: Margem bruta por SKU
CREATE OR REPLACE VIEW gold.vw_Q4_margem_sku AS
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
FROM gold.fato_vendas f
JOIN gold.dim_produto p ON f.sk_produto = p.sk_produto
WHERE f.status_financeiro = 'PAGO'
  AND f.receita_liquida > 0
GROUP BY p.sk_produto, p.nome_produto;
