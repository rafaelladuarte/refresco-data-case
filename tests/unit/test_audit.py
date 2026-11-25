"""pipeline.common.audit: linhas de auditoria, agregação de motivos e gravação idempotente."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from pipeline.common import audit


def quarentena(*motivos):
    return pd.DataFrame({"id": range(len(motivos)), "motivo_rejeicao": list(motivos)})


class TestMotivosAgregados:
    def test_remove_o_valor_ofensor_e_soma_a_mesma_regra(self):
        motivos = pd.Series(
            ["id_cliente inválido ou vazio: 'X1'", "id_cliente inválido ou vazio: 'Y2'", "cnpj vazio"]
        )
        assert audit.motivos_agregados(motivos) == {"cnpj vazio": 1, "id_cliente inválido ou vazio": 2}

    def test_cnpj_duplicado_agrupa_independente_do_id_canonico(self):
        motivos = pd.Series(["CNPJ duplicado de C0001", "CNPJ duplicado de C0002"])
        assert audit.motivos_agregados(motivos) == {"CNPJ duplicado": 2}

    def test_motivo_combinado_conta_em_cada_regra(self):
        motivos = pd.Series(["qtd_caixas deve ser > 0 | id_cliente vazio", "qtd_caixas deve ser > 0"])
        assert audit.motivos_agregados(motivos) == {"id_cliente vazio": 1, "qtd_caixas deve ser > 0": 2}

    def test_mesma_regra_repetida_no_mesmo_registro_conta_uma_vez(self):
        assert audit.motivos_agregados(pd.Series(["a: 1 | a: 2"])) == {"a": 1}

    def test_nulos_e_vazio(self):
        assert audit.motivos_agregados(pd.Series([None, float("nan")], dtype="object")) == {}
        assert audit.motivos_agregados(pd.Series([], dtype="object")) == {}


class TestBuildRows:
    def test_resumo_com_duplicadas_e_uma_linha_por_motivo(self):
        linhas = audit.build_rows(
            "vendas", "2024-06-01", "run-1", lidas=10, aprovadas=6,
            quarentena=quarentena("a: x", "a: y", "b"),
        )  # fmt: skip
        resumo, *motivos = linhas
        assert (resumo["lidas"], resumo["aprovadas"], resumo["duplicadas"], resumo["rejeitadas"]) == (
            10,
            6,
            1,
            3,
        )
        assert resumo["motivo"] is None
        assert {(m["motivo"], m["rejeitadas"]) for m in motivos} == {("a", 2), ("b", 1)}
        assert all(m["lidas"] is None for m in motivos)
        assert {r["run_id"] for r in linhas} == {"run-1"}
        assert {r["data_logica"] for r in linhas} == {"2024-06-01"}

    def test_sem_rejeicao_so_o_resumo(self):
        linhas = audit.build_rows("clima", "2024-06-01", "r", 5, 5, quarentena())
        assert len(linhas) == 1
        assert linhas[0]["rejeitadas"] == 0
        assert linhas[0]["duplicadas"] == 0

    def test_quarentena_sem_coluna_de_motivo_gera_so_o_resumo(self):
        linhas = audit.build_rows("x", "2024-06-01", "r", 3, 2, pd.DataFrame({"id": [1]}))
        assert len(linhas) == 1
        assert linhas[0]["rejeitadas"] == 1

    def test_aprovadas_mais_rejeitadas_acima_de_lidas_e_erro(self):
        with pytest.raises(ValueError, match="menor que"):
            audit.build_rows("x", "2024-06-01", "r", lidas=3, aprovadas=3, quarentena=quarentena("a"))


class TestRunId:
    def test_usa_o_do_airflow(self):
        assert audit.resolve_run_id("scheduled__2024-06-01T00:00:00+00:00", "2024-06-01") == (
            "scheduled__2024-06-01T00:00:00+00:00"
        )

    def test_execucao_manual_e_deterministica_por_data(self):
        assert audit.resolve_run_id(None, "2024-06-01") == "manual__2024-06-01"


class TestWriteAudit:
    def engine(self):
        conn = MagicMock()
        engine = MagicMock()
        engine.begin.return_value.__enter__.return_value = conn
        return engine, conn

    def test_apaga_as_linhas_do_run_e_entidade_e_insere_as_novas_na_mesma_transacao(self):
        engine, conn = self.engine()
        linhas = audit.build_rows("vendas", "2024-06-01", "run-1", 10, 7, quarentena("a", "b"))

        audit.write_audit(engine, linhas)

        engine.begin.assert_called_once()  # uma única transação
        sqls = [str(c.args[0]) for c in conn.execute.call_args_list]
        assert "pg_advisory_xact_lock" in sqls[0]
        assert sqls[1].startswith("DELETE FROM audit.dq_results")
        assert conn.execute.call_args_list[1].args[1] == {"run_id": "run-1", "entidade": "vendas"}
        assert sqls[2].startswith("INSERT INTO audit.dq_results")
        assert conn.execute.call_args_list[2].args[1] == linhas
        conn.exec_driver_sql.assert_called_once()  # DDL

    def test_ddl_versionada_cria_a_tabela_com_as_colunas_do_card(self):
        ddl = audit.DDL_PATH.read_text(encoding="utf-8")
        assert "CREATE TABLE IF NOT EXISTS audit.dq_results" in ddl
        for coluna in ("run_id", "entidade", "lidas", "aprovadas", "rejeitadas", "motivo", "registrado_em"):
            assert coluna in ddl


def test_record_dq_grava_o_resumo_com_o_run_id_resolvido():
    with (
        patch.object(audit, "create_engine") as create_engine,
        patch.object(audit, "postgres_url", return_value="postgresql://x"),
        patch.object(audit, "write_audit") as write_audit,
    ):
        audit.record_dq("clientes", "2024-06-01", None, 120, 115, quarentena("a", "a", "a", "a", "a"))
    create_engine.assert_called_once_with("postgresql://x")
    linhas = write_audit.call_args.args[1]
    assert linhas[0]["run_id"] == "manual__2024-06-01"
    assert (linhas[0]["lidas"], linhas[0]["aprovadas"], linhas[0]["rejeitadas"]) == (120, 115, 5)
