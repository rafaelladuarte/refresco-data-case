--Q1: Ranking de vendas por produto e região

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
    WHERE f.status_financeiro = 'PAGO'
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
        ROW_NUMBER() OVER (PARTITION BY regiao ORDER BY total_receita_liquida DESC) AS rank_produto,
        RANK() OVER (PARTITION BY regiao ORDER BY total_receita_liquida DESC) AS rank_empate
    FROM vendas_regiao_produto
)
SELECT 
    regiao,
    rank_produto AS ranking,
    nome_produto,
    familia,
    total_receita_liquida AS receita_liquida,
    total_vendas,
    total_caixas
FROM ranking_regiao
WHERE rank_produto <= 10
ORDER BY regiao, rank_produto;


--Q2: Volume realizado vs meta por canal 
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
    WHERE f.status_financeiro = 'PAGO'
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


--Q3: Análise de tendência de volume por produto
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
    WHERE f.status_financeiro = 'PAGO'
      AND t.data_referencia >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY p.nome_produto, p.sk_produto, t.data_referencia
),
analise_tendencia AS (
    SELECT 
        nome_produto,
        sk_produto,
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
        REGR_SLOPE(volume_diario_caixas, EXTRACT(EPOCH FROM data_referencia)) OVER (
            PARTITION BY nome_produto 
            ORDER BY data_referencia 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS tendencia_7dias,
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
    ROUND(tendencia_7dias::NUMERIC, 4) AS inclinacao_tendencia,
    qtde_vendas_dia,
    CASE 
        WHEN volume_diario_caixas > media_movel_7dias AND media_movel_7dias > media_movel_30dias
        THEN 'TENDENCIA DE ALTA'
        WHEN volume_diario_caixas < media_movel_7dias AND media_movel_7dias < media_movel_30dias
        THEN 'TENDENCIA DE BAIXA'
        WHEN tendencia_7dias > 0 AND volume_diario_caixas > media_movel_7dias
        THEN 'ACELERACAO POSITIVA'
        WHEN tendencia_7dias < 0 AND volume_diario_caixas < media_movel_7dias
        THEN 'ACELERACAO NEGATIVA'
        ELSE 'ESTAVEL/VOLATILIDADE'
    END AS sinal_tendencia,
    TO_CHAR(receita_diaria, 'L999G999D99') AS receita_diaria
FROM analise_tendencia
WHERE volume_diario_caixas > 0
ORDER BY nome_produto, data_referencia DESC
LIMIT 1000;

--Q4: Margem bruta por SKU
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
    WHERE f.status_financeiro = 'PAGO'
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
    TO_CHAR(preco_tabela, 'L999G999D99') AS preco_tabela,
    TO_CHAR(custo_unitario, 'L999G999D99') AS custo_unitario,
    TO_CHAR(custo_total_apurado, 'L999G999G999D99') AS custo_total,
    ROUND(
        ((preco_tabela - custo_unitario) * 100.0 / NULLIF(preco_tabela, 0))::NUMERIC,
        2
    ) AS margem_teorica_percentual,
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
ORDER BY margem_bruta_percentual DESC, receita_liquida_total DESC;


-- Complemento Q4: Margem agregada por família e marca
SELECT 
    p.familia,
    p.marca,
    COUNT(DISTINCT p.sk_produto) AS qtde_skus,
    SUM(f.receita_liquida) AS receita_total,
    SUM(f.receita_liquida - COALESCE(f.custo_total, f.qtd_caixas * p.custo_unitario)) AS margem_total,
    ROUND(
        (SUM(f.receita_liquida - COALESCE(f.custo_total, f.qtd_caixas * p.custo_unitario)) * 100.0 
         / NULLIF(SUM(f.receita_liquida), 0))::NUMERIC, 
        2
    ) AS margem_percentual_media
FROM gold.fato_vendas f
JOIN gold.dim_produto p ON f.sk_produto = p.sk_produto
WHERE f.status_financeiro = 'PAGO'
GROUP BY p.familia, p.marca
ORDER BY margem_percentual_media DESC;