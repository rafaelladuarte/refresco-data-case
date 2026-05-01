CREATE TABLE IF NOT EXISTS load_stage.clientes
(
    id_cliente text COLLATE pg_catalog."default",
    nome_cliente text COLLATE pg_catalog."default",
    cnpj text COLLATE pg_catalog."default",
    canal text COLLATE pg_catalog."default",
    cidade text COLLATE pg_catalog."default",
    uf text COLLATE pg_catalog."default",
    representante text COLLATE pg_catalog."default",
    ativo boolean,
    _ingested_at timestamp without time zone,
    _source_file text COLLATE pg_catalog."default",
    regiao text COLLATE pg_catalog."default"
)

TABLESPACE pg_default;


-- DROP TABLE IF EXISTS load_stage.clima;

CREATE TABLE IF NOT EXISTS load_stage.clima
(
    "time" timestamp without time zone,
    temperature_2m_max double precision,
    precipitation_sum double precision,
    _ingested_at timestamp without time zone,
    _api_source text COLLATE pg_catalog."default",
    ano text COLLATE pg_catalog."default",
    mes text COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS load_stage.clima
    OWNER to uberlandia;


-- DROP TABLE IF EXISTS load_stage.financeiro;

CREATE TABLE IF NOT EXISTS load_stage.financeiro
(
    id_financeiro text COLLATE pg_catalog."default",
    id_venda text COLLATE pg_catalog."default",
    tipo_lancamento_padrao text COLLATE pg_catalog."default",
    competencia timestamp without time zone,
    status_padrao text COLLATE pg_catalog."default",
    ano text COLLATE pg_catalog."default",
    mes text COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS load_stage.financeiro
    OWNER to uberlandia;

-- DROP TABLE IF EXISTS load_stage.produtos;

CREATE TABLE IF NOT EXISTS load_stage.produtos
(
    id_produto text COLLATE pg_catalog."default",
    nome_produto text COLLATE pg_catalog."default",
    familia text COLLATE pg_catalog."default",
    marca text COLLATE pg_catalog."default",
    embalagem text COLLATE pg_catalog."default",
    volume_ml double precision,
    preco_tabela double precision,
    custo_unitario double precision,
    _ingested_at timestamp without time zone,
    _source_file text COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS load_stage.produtos
    OWNER to uberlandia;


-- DROP TABLE IF EXISTS load_stage.vendas;

CREATE TABLE IF NOT EXISTS load_stage.vendas
(
    id_venda text COLLATE pg_catalog."default",
    data_emissao timestamp without time zone,
    id_produto text COLLATE pg_catalog."default",
    id_cliente text COLLATE pg_catalog."default",
    qtd_caixas bigint,
    preco_unitario double precision,
    desconto_pct double precision,
    representante text COLLATE pg_catalog."default",
    canal_venda text COLLATE pg_catalog."default",
    _ingested_at timestamp without time zone,
    _source_file text COLLATE pg_catalog."default",
    receita_liquida double precision,
    ano text COLLATE pg_catalog."default",
    mes text COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS load_stage.vendas
    OWNER to uberlandia;