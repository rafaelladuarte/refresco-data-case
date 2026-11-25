import pandas as pd
import pytest

from pipeline.silver.clean_produtos import flag_valores_indisponiveis, validate_produtos


def produtos(custos, precos=None):
    precos = precos if precos is not None else [5.0] * len(custos)
    return pd.DataFrame(
        {
            "id_produto": [f"P{i:03d}" for i in range(1, len(custos) + 1)],
            "custo_unitario": custos,
            "preco_tabela": precos,
        }
    )


class TestFlagValoresIndisponiveis:
    def test_custo_positivo_mantido_e_disponivel(self):
        df = flag_valores_indisponiveis(produtos([2.35]))
        assert df["custo_unitario"].tolist() == [2.35]
        assert df["custo_disponivel"].tolist() == [True]

    @pytest.mark.parametrize("custo", [0.0, -1.5, None, float("nan"), "abc"])
    def test_custo_zero_negativo_nulo_ou_invalido_vira_nulo_com_flag(self, custo):
        df = flag_valores_indisponiveis(produtos([custo]))
        assert pd.isna(df["custo_unitario"].iloc[0])
        assert df["custo_disponivel"].tolist() == [False]

    @pytest.mark.parametrize("preco", [0.0, -2.0, None])
    def test_preco_invalido_segue_a_mesma_regra(self, preco):
        df = flag_valores_indisponiveis(produtos([1.0], precos=[preco]))
        assert pd.isna(df["preco_tabela"].iloc[0])
        assert df["preco_disponivel"].tolist() == [False]
        assert df["custo_disponivel"].tolist() == [True]

    def test_produto_nunca_e_descartado(self):
        # Caso real da origem: P012 e P019 com custo 0.00 continuam na Silver
        df = flag_valores_indisponiveis(produtos([1.2, 0.0, 3.1, 0.0]))
        assert df["id_produto"].tolist() == ["P001", "P002", "P003", "P004"]
        assert df["custo_disponivel"].tolist() == [True, False, True, False]

    def test_flags_sao_bool_nativo(self):
        # bool (não object/boolean nullable): vira boolean no Postgres e nunca é nulo
        df = flag_valores_indisponiveis(produtos([1.0, None]))
        assert df["custo_disponivel"].dtype == bool
        assert df["preco_disponivel"].dtype == bool

    def test_nao_altera_o_dataframe_de_entrada(self):
        entrada = produtos([0.0])
        flag_valores_indisponiveis(entrada)
        assert entrada["custo_unitario"].tolist() == [0.0]
        assert "custo_disponivel" not in entrada.columns

    def test_entrada_vazia(self):
        df = flag_valores_indisponiveis(produtos([]))
        assert df.empty
        assert {"custo_disponivel", "preco_disponivel"} <= set(df.columns)


class TestValidateProdutos:
    def test_id_valido_passa_e_invalido_vai_para_a_quarentena_com_motivo(self):
        df = pd.DataFrame({"id_produto": ["P001", "PXXX", "", "P12", "P019"], "nome_produto": list("abcde")})

        ok, quarentena = validate_produtos(df)

        assert ok["id_produto"].tolist() == ["P001", "P019"]
        assert sorted(quarentena["id_produto"].tolist()) == ["", "P12", "PXXX"]
        assert quarentena["motivo_rejeicao"].str.startswith("id_produto inválido ou vazio: '").all()
        assert quarentena.loc[quarentena["id_produto"] == "PXXX", "motivo_rejeicao"].iloc[0] == (
            "id_produto inválido ou vazio: 'PXXX'"
        )

    def test_todos_validos_gera_quarentena_vazia_com_a_coluna_de_motivo(self):
        ok, quarentena = validate_produtos(pd.DataFrame({"id_produto": ["P001", "P002"]}))
        assert len(ok) == 2
        assert quarentena.empty
        assert "motivo_rejeicao" in quarentena.columns

    def test_preserva_as_demais_colunas_na_quarentena(self):
        df = pd.DataFrame({"id_produto": ["P01"], "nome_produto": ["Suco"]})
        _, quarentena = validate_produtos(df)
        assert quarentena["nome_produto"].tolist() == ["Suco"]
