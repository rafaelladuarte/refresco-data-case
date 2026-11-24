import pandas as pd
import pytest

from pipeline.silver.clean_clientes import deduplicate_clientes, normalize_ativo, repair_double_encoding


class TestRepairDoubleEncoding:
    def test_corrige_mojibake_real(self):
        assert repair_double_encoding("Mercado do ZÃ©") == "Mercado do Zé"
        assert repair_double_encoding("ConveniÃªncia") == "Conveniência"

    @pytest.mark.parametrize("texto", ["São Paulo", "Conveniência", "Açaí", "Distribuidora"])
    def test_nao_altera_texto_legitimo(self, texto):
        assert repair_double_encoding(texto) == texto

    def test_conversao_invalida_devolve_o_original(self):
        # Tem o marcador "Ã", mas os bytes não formam UTF-8 válido
        assert repair_double_encoding("ÃNGELA") == "ÃNGELA"

    @pytest.mark.parametrize("valor", [None, 123])
    def test_nao_texto_passa_direto(self, valor):
        assert repair_double_encoding(valor) == valor

    def test_nan_passa_direto(self):
        assert pd.isna(repair_double_encoding(float("nan")))


class TestNormalizeAtivo:
    @pytest.mark.parametrize("valor", ["S", " s ", "Sim", "SIM", "1", 1, "true", "T", "yes"])
    def test_verdadeiros(self, valor):
        assert normalize_ativo(valor) is True

    @pytest.mark.parametrize("valor", ["N", "nao", "Não", "NÃO", "0", 0, "false", "F", "no"])
    def test_falsos(self, valor):
        assert normalize_ativo(valor) is False

    @pytest.mark.parametrize("valor", ["talvez", "", "2", None, float("nan"), pd.NA])
    def test_desconhecido_ou_nulo_vira_none(self, valor):
        assert normalize_ativo(valor) is None


def cliente(id_cliente, cnpj="11.111.111/0001-11", nome="Cliente", ingested="2024-01-01"):
    return {"id_cliente": id_cliente, "cnpj": cnpj, "nome_cliente": nome, "_ingested_at": pd.Timestamp(ingested)}


def dedup(*linhas):
    return deduplicate_clientes(pd.DataFrame(list(linhas)))


class TestDeduplicateClientes:
    def test_mantem_o_registro_mais_recente_por_id(self):
        ok, quarentena = dedup(
            cliente("C0001", nome="Antigo", ingested="2024-01-01"),
            cliente("C0001", nome="Novo", ingested="2024-03-01"),
        )
        assert ok["nome_cliente"].tolist() == ["Novo"]
        assert quarentena.empty

    def test_empate_de_ingestao_mantem_a_ultima_linha_lida(self):
        ok, _ = dedup(cliente("C0001", nome="Primeira"), cliente("C0001", nome="Segunda"))
        assert ok["nome_cliente"].tolist() == ["Segunda"]

    def test_cnpj_duplicado_mantem_o_menor_id(self):
        ok, quarentena = dedup(cliente("C0200", cnpj="X"), cliente("C0001", cnpj="X"), cliente("C0002", cnpj="Y"))
        assert ok["id_cliente"].tolist() == ["C0001", "C0002"]
        assert quarentena["id_cliente"].tolist() == ["C0200"]
        assert quarentena["motivo_rejeicao"].tolist() == ["CNPJ duplicado de C0001"]
        assert "_id_canonico" not in ok.columns and "_id_canonico" not in quarentena.columns

    def test_resultado_deterministico_em_ordens_embaralhadas(self):
        linhas = [
            cliente("C0003", cnpj="X", ingested="2024-01-01"),
            cliente("C0001", cnpj="X", ingested="2024-01-02"),
            cliente("C0002", cnpj="Y", nome="v1", ingested="2024-01-01"),
            cliente("C0002", cnpj="Y", nome="v2", ingested="2024-02-01"),
            cliente("C0004", cnpj="Z"),
        ]
        esperado_ok, esperado_q = dedup(*linhas)
        df = pd.DataFrame(linhas)
        for seed in range(5):
            ok, q = deduplicate_clientes(df.sample(frac=1, random_state=seed).reset_index(drop=True))
            pd.testing.assert_frame_equal(ok, esperado_ok)
            assert sorted(q["id_cliente"]) == sorted(esperado_q["id_cliente"])

    @pytest.mark.parametrize("id_invalido", ["", None, "CLI01", "C01"])
    def test_id_invalido_vai_para_quarentena(self, id_invalido):
        ok, quarentena = dedup(cliente(id_invalido), cliente("C0001", cnpj="outro"))
        assert ok["id_cliente"].tolist() == ["C0001"]
        assert quarentena["motivo_rejeicao"].iloc[0].startswith("id_cliente inválido ou vazio")

    def test_id_minusculo_com_espacos_e_normalizado(self):
        ok, quarentena = dedup(cliente(" c0007 "))
        assert ok["id_cliente"].tolist() == ["C0007"] and quarentena.empty

    @pytest.mark.parametrize("campo, motivo", [("cnpj", "cnpj vazio"), ("nome", "nome_cliente vazio")])
    def test_campos_obrigatorios(self, campo, motivo):
        ok, quarentena = dedup(cliente("C0001", **{campo: None}))
        assert ok.empty
        assert quarentena["motivo_rejeicao"].tolist() == [motivo]

    def test_sem_rejeicoes_quarentena_vazia_com_coluna_de_motivo(self):
        _, quarentena = dedup(cliente("C0001"))
        assert quarentena.empty and "motivo_rejeicao" in quarentena.columns

    def test_saida_ordenada_por_id_com_indice_novo(self):
        ok, _ = dedup(cliente("C0003", cnpj="A"), cliente("C0001", cnpj="B"), cliente("C0002", cnpj="C"))
        assert ok["id_cliente"].tolist() == ["C0001", "C0002", "C0003"]
        assert ok.index.tolist() == [0, 1, 2]
