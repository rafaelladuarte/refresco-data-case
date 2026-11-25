"""pipeline.load.staging: DDL tipada, conversão para COPY e carga transacional (sem Postgres)."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from pipeline.load import staging

SPEC = staging.parse_ddl(staging.DDL_PATH.read_text(encoding="utf-8"))


class TestParseDdl:
    def test_a_ddl_versionada_define_as_cinco_tabelas_do_loader(self):
        assert set(SPEC) == set(staging.SILVER_PATHS)

    def test_tipos_explicitos_ano_e_mes_sao_inteiros(self):
        for tabela in ("vendas", "financeiro", "clima"):
            tipos = dict(SPEC[tabela])
            assert tipos["ano"] == "integer"
            assert tipos["mes"] == "integer"

    def test_colunas_de_controle_em_todas_as_tabelas(self):
        for tabela, colunas in SPEC.items():
            nomes = {c for c, _ in colunas}
            assert {"_ingested_at", "_logical_date", "_row_hash", "_changed_at"} <= nomes, tabela

    def test_extrai_colunas_na_ordem_e_tipos(self):
        sql = """
            CREATE SCHEMA IF NOT EXISTS load_stage;
            CREATE TABLE IF NOT EXISTS load_stage.t (
                a text,
                b double precision,
                c timestamp
            );
        """
        assert staging.parse_ddl(sql) == {"t": [("a", "text"), ("b", "double precision"), ("c", "timestamp")]}

    def test_tipo_nao_suportado_falha(self):
        with pytest.raises(ValueError, match="Tipo não suportado"):
            staging.parse_ddl("CREATE TABLE IF NOT EXISTS load_stage.t (a jsonb);")


class TestValidateColumns:
    ESPEC = [("a", "text"), ("b", "integer")]

    def test_mesmas_colunas_em_qualquer_ordem_passa(self):
        staging.validate_columns("t", ["b", "a"], self.ESPEC)

    def test_coluna_nova_na_silver_falha_nomeando_a_diferenca(self):
        with pytest.raises(ValueError, match=r"sobram na Silver: \['nova'\]"):
            staging.validate_columns("t", ["a", "b", "nova"], self.ESPEC)

    def test_coluna_ausente_na_silver_falha_nomeando_a_diferenca(self):
        with pytest.raises(ValueError, match=r"Faltam na Silver: \['b'\]"):
            staging.validate_columns("t", ["a"], self.ESPEC)

    def test_a_silver_real_de_cada_tabela_precisa_bater_com_a_ddl(self):
        """Contrato: as colunas que cada main() Silver produz estão todas na DDL."""
        for tabela, colunas in SPEC.items():
            staging.validate_columns(tabela, [c for c, _ in colunas], colunas)


class TestPrepareFrame:
    def test_ordena_como_a_ddl_e_converte_os_tipos(self):
        spec = [("id", "text"), ("qtd", "integer"), ("preco", "double precision"), ("ok", "boolean")]
        df = pd.DataFrame({"ok": [True, None], "preco": ["1.5", "2"], "qtd": [3.0, None], "id": ["a", "b"]})

        pronto = staging.prepare_frame(df, spec)

        assert list(pronto.columns) == ["id", "qtd", "preco", "ok"]
        assert str(pronto["qtd"].dtype) == "Int64"
        assert pronto["qtd"].isna().tolist() == [False, True]
        assert pronto["preco"].tolist() == [1.5, 2.0]
        assert str(pronto["ok"].dtype) == "boolean"

    def test_colunas_de_particao_categoricas_viram_inteiro(self):
        """O pandas relê ano/mes particionados como categóricas: antes viravam `text` no Postgres."""
        df = pd.DataFrame({"ano": pd.Categorical([2024, 2025]), "mes": pd.Categorical([1, 12])})
        pronto = staging.prepare_frame(df, [("ano", "integer"), ("mes", "integer")])
        assert pronto["ano"].tolist() == [2024, 2025]
        assert str(pronto["ano"].dtype) == "Int64"

    def test_categorica_de_texto_vira_texto(self):
        df = pd.DataFrame({"regiao": pd.Categorical(["Sul de Minas"])})
        assert staging.prepare_frame(df, [("regiao", "text")])["regiao"].tolist() == ["Sul de Minas"]

    def test_timestamp(self):
        df = pd.DataFrame({"t": ["2024-02-28 00:00:00", "2024-03-01 10:30:00"]})
        pronto = staging.prepare_frame(df, [("t", "timestamp")])
        assert pronto["t"].dtype.kind == "M"


class TestCopyBuffer:
    def csv(self, df):
        return staging.to_copy_buffer(df).read()

    def test_nulo_usa_o_marcador_e_texto_vazio_continua_vazio(self):
        df = pd.DataFrame(
            {"a": pd.array(["x", None, ""], dtype="string"), "n": pd.array([1, None, 3], dtype="Int64")}
        )
        linhas = self.csv(df).splitlines()
        assert linhas == ["x,1", "\\N,\\N", ",3"]

    def test_campos_com_virgula_aspas_e_quebra_de_linha_sao_escapados(self):
        df = pd.DataFrame({"a": pd.array(["com,virgula", 'com "aspas"', "duas\nlinhas"], dtype="string")})
        assert self.csv(df) == '"com,virgula"\n"com ""aspas"""\n"duas\nlinhas"\n'

    def test_sem_cabecalho_nem_indice(self):
        assert self.csv(pd.DataFrame({"a": [1]})) == "1\n"

    def test_timestamp_e_booleano_no_formato_do_postgres(self):
        df = pd.DataFrame(
            {"t": pd.to_datetime(["2024-02-28 10:30:00"]), "b": pd.array([True], dtype="boolean")}
        )
        assert self.csv(df) == "2024-02-28 10:30:00.000000,True\n"


class TestTabelasDivergentes:
    def conn(self, linhas):
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = linhas
        return conn

    SPEC_T = {"t": [("a", "text"), ("n", "integer"), ("d", "timestamp")]}

    def test_tabela_igual_a_ddl_nao_diverge(self):
        conn = self.conn(
            [("t", "a", "text"), ("t", "n", "integer"), ("t", "d", "timestamp without time zone")]
        )
        assert staging.tabelas_divergentes(conn, self.SPEC_T) == []

    def test_tipo_diferente_diverge(self):
        """Tabela criada pelo pandas: `n` era bigint/text."""
        conn = self.conn([("t", "a", "text"), ("t", "n", "text"), ("t", "d", "timestamp without time zone")])
        assert staging.tabelas_divergentes(conn, self.SPEC_T) == ["t"]

    def test_coluna_a_mais_ou_a_menos_diverge(self):
        conn = self.conn([("t", "a", "text"), ("t", "n", "integer")])
        assert staging.tabelas_divergentes(conn, self.SPEC_T) == ["t"]

    def test_tabela_inexistente_nao_e_divergente(self):
        assert staging.tabelas_divergentes(self.conn([]), self.SPEC_T) == []


class TestLoadToStaging:
    @pytest.fixture
    def ambiente(self):
        """Silver fictícia com as colunas exatas da DDL e um Postgres simulado que registra o SQL."""
        eventos = []

        def silver(caminho):
            tabela = next(t for t, p in staging.SILVER_PATHS.items() if p == caminho)
            return pd.DataFrame({nome: [None] for nome, _ in SPEC[tabela]})

        cursor = MagicMock()
        cursor.execute.side_effect = lambda sql: eventos.append(("sql", sql))
        cursor.copy_expert.side_effect = lambda sql, buf: eventos.append(("copy", sql))

        conn = MagicMock()
        conn.connection.cursor.return_value = cursor
        conn.exec_driver_sql.side_effect = lambda sql: eventos.append(("ddl", "CREATE" in sql))
        conn.execute.return_value.fetchall.return_value = []
        engine = MagicMock()
        engine.begin.return_value.__enter__.return_value = conn
        engine.begin.return_value.__exit__.return_value = False  # não engole exceções

        with (
            patch.object(staging, "read_silver", side_effect=silver),
            patch.object(staging, "create_engine", return_value=engine),
            patch.object(staging, "postgres_url", return_value="postgresql://x"),
        ):
            yield eventos, engine

    def test_aplica_a_ddl_e_depois_faz_truncate_e_copy_de_cada_tabela_na_mesma_transacao(self, ambiente):
        eventos, engine = ambiente

        staging.load_to_staging()

        engine.begin.assert_called_once()  # uma única transação para as cinco tabelas
        assert eventos[0] == ("ddl", True)
        resto = [e for e in eventos[1:]]
        assert len(resto) == 10  # 5 x (TRUNCATE, COPY)
        for (tipo_a, sql_a), (tipo_b, sql_b) in zip(resto[::2], resto[1::2], strict=True):
            assert tipo_a == "sql" and sql_a.startswith("TRUNCATE TABLE load_stage.")
            assert tipo_b == "copy" and "FROM STDIN WITH (FORMAT csv, NULL '\\N')" in sql_b
            assert sql_a.split()[-1] == sql_b.split()[1]  # TRUNCATE e COPY miram a mesma tabela

    def test_nunca_derruba_tabelas_em_execucao_normal(self, ambiente):
        eventos, engine = ambiente
        staging.load_to_staging()
        conn = engine.begin.return_value.__enter__.return_value
        assert not [c for c in conn.execute.call_args_list if "DROP TABLE" in str(c.args[0])]

    def test_tabela_divergente_e_recriada_uma_vez_com_aviso(self, ambiente, caplog):
        eventos, engine = ambiente
        with (
            patch.object(staging, "tabelas_divergentes", return_value=["vendas"]),
            caplog.at_level("WARNING"),
        ):
            staging.load_to_staging()
        conn = engine.begin.return_value.__enter__.return_value
        drops = [str(c.args[0]) for c in conn.execute.call_args_list if "DROP TABLE" in str(c.args[0])]
        assert drops == ["DROP TABLE load_stage.vendas CASCADE"]
        assert any("criada antes do ARQ-03" in r.message for r in caplog.records)

    def test_desvio_de_schema_na_silver_falha_antes_de_abrir_a_transacao(self, ambiente):
        _, engine = ambiente
        original = staging.read_silver

        def silver_com_coluna_nova(caminho):
            df = original(caminho)
            return df.assign(coluna_inesperada=1) if caminho.endswith("clientes/") else df

        with (
            patch.object(staging, "read_silver", silver_com_coluna_nova),
            pytest.raises(ValueError, match="coluna_inesperada"),
        ):
            staging.load_to_staging()
        engine.begin.assert_not_called()  # nenhum lock, nada carregado

    def test_falha_no_copy_propaga_o_erro_para_o_rollback(self, ambiente):
        _, engine = ambiente
        with (
            patch.object(staging, "_copy", side_effect=[None, RuntimeError("COPY falhou")]),
            pytest.raises(RuntimeError, match="COPY falhou"),
        ):
            staging.load_to_staging()
        # o context manager de engine.begin() é quem faz o rollback: o erro precisa sair por ele
        assert engine.begin.return_value.__exit__.call_args.args[0] is RuntimeError
