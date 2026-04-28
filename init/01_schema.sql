-- Schema inicial para a camada Gold

CREATE TABLE IF NOT EXISTS gold_vendas (
    id SERIAL PRIMARY KEY,
    data_venda DATE,
    cliente_id INT,
    produto_id INT,
    quantidade INT,
    valor_total DECIMAL(10,2)
);

-- Adicionar outras tabelas conforme necessário
