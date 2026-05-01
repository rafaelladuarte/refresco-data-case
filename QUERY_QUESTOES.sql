--Q1: Ranking de vendas por produto e região

WITH receita_por_produto AS (
    SELECT 
        c.regiao,
        p.nome_produto,
        SUM(v.receita_liquida) AS receita_total
    FROM silver_stage.vendas AS v
    JOIN silver_stage.clientes AS c ON v.id_cliente = c.id_cliente
    JOIN silver_stage.produtos AS p ON v.id_produto = p.id_produto
    GROUP BY 1, 2
),
ranking_produtos AS (
    SELECT 
        regiao,
        nome_produto,
        ROUND(receita_total::numeric, 2) AS receita_total,
        RANK() OVER (PARTITION BY regiao ORDER BY receita_total DESC) AS rank_posicao
    FROM receita_por_produto
)
SELECT *
INTO questao1
FROM ranking_produtos WHERE rank_posicao <= 5;

---- PRODUTO ESTRELA?
SELECT nome_produto, count(*) AS qtd
FROM questao1
GROUP BY nome_produto
ORDER BY COUNT(*) DESC

---- DEMANDA POR REGIAO?
---- RISCO DE PORFOLIO?

--Q2: Volume realizado vs meta por canal 
-- tabela de meta?


SELECT 
    c.canal,
    ROUND(SUM(v.qtd_caixas),2) AS volume_total
FROM silver_stage.vendas v
JOIN silver_stage.clientes c 
    ON v.id_cliente = c.id_cliente
GROUP BY c.canal
ORDER BY volume_total DESC;

-- canal nulo? investigar


--Q3: Análise de tendência de volume por produto
WITH volume_diario AS (
    SELECT 
        CAST(v.data_emissao AS DATE) AS data_venda,
        p.nome_produto,
        SUM(v.qtd_caixas) AS volume_dia
    FROM silver_stage.vendas AS v
    JOIN silver_stage.produtos p ON v.id_produto = p.id_produto
    GROUP BY 1, 2
),
tendencia AS (
    SELECT 
        data_venda,
        nome_produto,
        volume_dia,
        ROUND(AVG(volume_dia) OVER (
            PARTITION BY nome_produto 
            ORDER BY data_venda 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) ,2)AS media_movel_7d,
        LAG(volume_dia) OVER (
            PARTITION BY nome_produto 
            ORDER BY data_venda
        ) AS volume_dia_anterior
    FROM volume_diario
)
SELECT 
    *,
    (volume_dia - volume_dia_anterior) AS diferenca_vs_dia_anterior
FROM tendencia;


---- Queda ou crescimento?
---- Presença de picos? --> relacionar com clima

--Q4: Margem bruta por SKU
SELECT 
    v.id_produto AS sku,
    p.nome_produto,
    ROUND(SUM(v.receita_liquida)::numeric, 2) AS receita_total,
    ROUND(SUM(v.qtd_caixas * p.custo_unitario)::numeric, 2) AS custo_total,
    ROUND(
        (SUM(v.receita_liquida) - SUM(v.qtd_caixas * p.custo_unitario))::numeric,
        2
    ) AS margem_bruta_valor,
    ROUND(
        (
            (SUM(v.receita_liquida) - SUM(v.qtd_caixas * p.custo_unitario)) 
            / NULLIF(SUM(v.receita_liquida), 0)
        )::numeric * 100,
        2
    ) AS percentual_margem
FROM silver_stage.vendas AS v
JOIN silver_stage.produtos AS p ON v.id_produto = p.id_produto
GROUP BY 1, 2;
