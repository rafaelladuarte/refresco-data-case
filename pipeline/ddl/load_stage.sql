-- Tabelas de carga (load_stage): espelho tipado da Silver, lido pelas views stg_* do dbt (ARQ-03).
--
-- O loader (pipeline/load/staging.py) aplica este arquivo e depois, numa única transação, faz
-- TRUNCATE + COPY de cada tabela. Como as tabelas nunca são recriadas, as views stg_* sobrevivem.
--
-- Contrato: uma coluna por linha, "nome tipo,". O loader lê este arquivo para conferir as colunas da
-- Silver e para converter os tipos antes do COPY. Tipos aceitos: text, integer, double precision,
-- boolean, timestamp.
--
-- Medidas ficam em `double precision` (como o pandas gravava): trocar por `numeric` mudaria a
-- representação dos valores lidos pelo dbt.

CREATE SCHEMA IF NOT EXISTS load_stage;

CREATE TABLE IF NOT EXISTS load_stage.clientes (
    id_cliente text,
    nome_cliente text,
    cnpj text,
    canal text,
    cidade text,
    uf text,
    representante text,
    ativo boolean,
    regiao text,
    _ingested_at timestamp,
    _source_file text,
    _logical_date text,
    _row_hash text,
    _changed_at timestamp
);

CREATE TABLE IF NOT EXISTS load_stage.produtos (
    id_produto text,
    nome_produto text,
    familia text,
    marca text,
    embalagem text,
    volume_ml double precision,
    preco_tabela double precision,
    custo_unitario double precision,
    custo_disponivel boolean,
    preco_disponivel boolean,
    _ingested_at timestamp,
    _source_file text,
    _logical_date text,
    _row_hash text,
    _changed_at timestamp
);

CREATE TABLE IF NOT EXISTS load_stage.vendas (
    id_venda text,
    data_emissao timestamp,
    id_produto text,
    id_cliente text,
    qtd_caixas integer,
    preco_unitario double precision,
    desconto_pct double precision,
    representante text,
    canal_venda text,
    receita_liquida double precision,
    ano integer,
    mes integer,
    _ingested_at timestamp,
    _source_file text,
    _logical_date text,
    _row_hash text,
    _changed_at timestamp
);

CREATE TABLE IF NOT EXISTS load_stage.financeiro (
    id_financeiro text,
    id_venda text,
    tipo_lancamento_padrao text,
    competencia timestamp,
    status_padrao text,
    ano integer,
    mes integer,
    _ingested_at timestamp,
    _logical_date text,
    _row_hash text,
    _changed_at timestamp
);

CREATE TABLE IF NOT EXISTS load_stage.clima (
    time timestamp,
    temperature_2m_max double precision,
    precipitation_sum double precision,
    ano integer,
    mes integer,
    _api_source text,
    _ingested_at timestamp,
    _logical_date text,
    _row_hash text,
    _changed_at timestamp
);
