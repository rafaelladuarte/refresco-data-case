-- Auditoria de qualidade de dados (ARQ-05). Aplicada de forma idempotente pelas tasks Silver.
--
-- Uma execução (run_id) de uma entidade grava:
--   * 1 linha-resumo (motivo IS NULL) com lidas, aprovadas, duplicadas e rejeitadas;
--   * 1 linha por motivo de rejeição (lidas/aprovadas/duplicadas NULL; rejeitadas = registros que
--     violaram aquela regra; um registro com várias violações conta em cada uma).
--
-- Invariante da linha-resumo: lidas = aprovadas + duplicadas + rejeitadas.
-- `duplicadas` são linhas descartadas por repetirem a chave natural dentro da partição (vale a
-- versão mais recente).

CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS audit.dq_results (
    id            bigserial PRIMARY KEY,
    run_id        text        NOT NULL,
    entidade      text        NOT NULL,
    data_logica   date        NOT NULL,
    lidas         integer,
    aprovadas     integer,
    duplicadas    integer,
    rejeitadas    integer     NOT NULL,
    motivo        text,
    registrado_em timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS dq_results_run_entidade_idx ON audit.dq_results (run_id, entidade);
