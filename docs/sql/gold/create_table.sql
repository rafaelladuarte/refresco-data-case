-- ==========================================
-- DIMENSÕES
-- ==========================================

CREATE TABLE dw.dim_cliente (
    sk_cliente      SERIAL PRIMARY KEY,
    id_cliente      TEXT NOT NULL,
    nome_cliente    TEXT,
    cnpj            TEXT,
    canal           TEXT,
    cidade          TEXT,
    uf              TEXT,
    regiao          TEXT,
    representante   TEXT,
    ativo           BOOLEAN,
    _source_file    TEXT,
    _ingested_at    TIMESTAMP
);

-- ==========================================

CREATE TABLE dw.dim_produto (
    sk_produto      SERIAL PRIMARY KEY,
    id_produto      TEXT NOT NULL,
    nome_produto    TEXT,
    familia         TEXT,
    marca           TEXT,
    embalagem       TEXT,
    volume_ml       DOUBLE PRECISION,
    preco_tabela    DOUBLE PRECISION,
    custo_unitario  DOUBLE PRECISION,
    _source_file    TEXT,
    _ingested_at    TIMESTAMP
);

-- ==========================================

CREATE TABLE dw.dim_representante (
    sk_representante SERIAL PRIMARY KEY,
    nome_representante TEXT NOT NULL,
    canal_venda        TEXT
);

-- ==========================================

-- dim_tempo com clima embutido no nível mensal
CREATE TABLE dw.dim_tempo (
    sk_tempo          INT PRIMARY KEY,  -- formato YYYYMMDD
    data_completa     DATE NOT NULL,
    ano               INT,
    mes               INT,
    dia               INT,
    trimestre         INT,
    nome_mes          TEXT,             -- 'Janeiro', 'Fevereiro'...
    dia_semana        TEXT,             -- 'Segunda', 'Terça'...
    is_dia_util       BOOLEAN,
    clima_temp_max    DOUBLE PRECISION, -- temperature_2m_max
    clima_precipitacao DOUBLE PRECISION -- precipitation_sum
);

-- ==========================================
-- FATO
-- ==========================================

CREATE TABLE dw.fato_vendas (
    sk_venda            SERIAL PRIMARY KEY,
    id_venda            TEXT,               -- chave natural, para rastreabilidade
    sk_cliente          INT NOT NULL REFERENCES dw.dim_cliente(sk_cliente),
    sk_produto          INT NOT NULL REFERENCES dw.dim_produto(sk_produto),
    sk_tempo            INT NOT NULL REFERENCES dw.dim_tempo(sk_tempo),
    sk_representante    INT NOT NULL REFERENCES dw.dim_representante(sk_representante),
    -- métricas
    qtd_caixas          BIGINT,
    preco_unitario      DOUBLE PRECISION,
    desconto_pct        DOUBLE PRECISION,
    receita_liquida     DOUBLE PRECISION,
    custo_total         DOUBLE PRECISION,   -- qtd_caixas * custo_unitario (calculado no ETL)
    margem_bruta        DOUBLE PRECISION,   -- receita_liquida - custo_total
    -- status financeiro (desnormalizado de load_stage.financeiro)
    tipo_lancamento     TEXT,
    status_financeiro   TEXT,
    -- controle
    _source_file        TEXT,
    _ingested_at        TIMESTAMP
);