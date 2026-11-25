import pandas as pd
import pytest

from pipeline.silver.clean_vendas import parse_mixed_dates, validate_vendas


class TestParseMixedDates:
    @pytest.mark.parametrize(
        "texto",
        ["2024-02-01", "01/02/2024", "01/02/24", "01-02-2024"],
    )
    def test_quatro_formatos_aceitos(self, texto):
        assert parse_mixed_dates(pd.Series([texto]))[0] == pd.Timestamp("2024-02-01")

    @pytest.mark.parametrize("texto", ["31/02/2024", "2024-13-01", "ontem", "", None, "2024/02/01"])
    def test_invalidos_viram_nat(self, texto):
        assert pd.isna(parse_mixed_dates(pd.Series([texto], dtype="object"))[0])

    def test_formato_br_e_dia_mes_nunca_mes_dia(self):
        # 03/04 é 3 de abril (BR), não 4 de março
        assert parse_mixed_dates(pd.Series(["03/04/2024"]))[0] == pd.Timestamp("2024-04-03")

    def test_formatos_nao_se_confundem(self):
        # Ano de 2 dígitos não pode ser lido como ano 0024 nem o de 4 dígitos truncado
        result = parse_mixed_dates(pd.Series(["01/02/24", "01/02/2024"]))
        assert result.tolist() == [pd.Timestamp("2024-02-01")] * 2

    def test_serie_mista_preserva_indice(self):
        s = pd.Series(["2024-01-05", "lixo", "06/01/2024"], index=[10, 20, 30])
        result = parse_mixed_dates(s)
        assert result.index.tolist() == [10, 20, 30]
        assert result[10] == pd.Timestamp("2024-01-05")
        assert pd.isna(result[20])
        assert result[30] == pd.Timestamp("2024-01-06")


CLIENTES = pd.DataFrame({"id_cliente": ["C0001", "C0002"]})
PRODUTOS = pd.DataFrame({"id_produto": ["P001", "P002"]})


def venda(**override) -> dict:
    base = {
        "id_venda": "V000001",
        "data_emissao": pd.Timestamp("2024-01-10"),
        "data_emissao_original": "2024-01-10",
        "id_cliente": "C0001",
        "id_produto": "P001",
        "qtd_caixas": 10,
        "preco_unitario": 5.0,
        "desconto_pct": 0.1,
    }
    base.update(override)
    return base


def validar(*vendas):
    df = pd.DataFrame(list(vendas))
    df["data_emissao_original"] = df["data_emissao_original"].astype("string")
    return validate_vendas(df, CLIENTES, PRODUTOS)


def motivo_unico(**override) -> str:
    ok, quarentena = validar(venda(**override))
    assert ok.empty and len(quarentena) == 1
    return quarentena["motivo_rejeicao"].iloc[0]


class TestValidateVendas:
    def test_venda_valida_passa_sem_coluna_auxiliar(self):
        ok, quarentena = validar(venda())
        assert len(ok) == 1 and quarentena.empty
        assert "data_emissao_original" not in ok.columns

    @pytest.mark.parametrize(
        "override, esperado",
        [
            ({"id_venda": ""}, "id_venda vazio"),
            (
                {"data_emissao": pd.NaT, "data_emissao_original": "31/02/2024"},
                "data_emissao inválida: '31/02/2024'",
            ),
            ({"data_emissao": pd.NaT, "data_emissao_original": None}, "data_emissao inválida: '<nulo>'"),
            ({"qtd_caixas": 0}, "qtd_caixas deve ser > 0: '0'"),
            ({"qtd_caixas": -3}, "qtd_caixas deve ser > 0: '-3'"),
            ({"preco_unitario": 0.0}, "preco_unitario deve ser > 0: '0.0'"),
            ({"desconto_pct": 1.0}, "desconto_pct fora de [0, 1): '1.0'"),
            ({"desconto_pct": -0.1}, "desconto_pct fora de [0, 1): '-0.1'"),
            ({"id_cliente": ""}, "id_cliente vazio"),
            ({"id_cliente": "CLI-1"}, "id_cliente formato inválido: 'CLI-1'"),
            ({"id_cliente": "C9999"}, "id_cliente sem cadastro: 'C9999'"),
            ({"id_produto": None}, "id_produto vazio"),
            ({"id_produto": "P1"}, "id_produto formato inválido: 'P1'"),
            ({"id_produto": "P999"}, "id_produto sem cadastro: 'P999'"),
        ],
    )
    def test_cada_regra_isolada(self, override, esperado):
        assert motivo_unico(**override) == esperado

    @pytest.mark.parametrize("campo", ["qtd_caixas", "preco_unitario", "desconto_pct"])
    def test_medida_nula_e_rejeitada(self, campo):
        assert motivo_unico(**{campo: None}).startswith(campo)

    def test_desconto_zero_e_permitido(self):
        ok, _ = validar(venda(desconto_pct=0.0))
        assert len(ok) == 1

    def test_varias_violacoes_combinam_motivos_na_ordem_das_regras(self):
        motivo = motivo_unico(qtd_caixas=0, id_cliente="", id_produto="P999")
        assert motivo == "qtd_caixas deve ser > 0: '0' | id_cliente vazio | id_produto sem cadastro: 'P999'"

    def test_ids_minusculos_e_com_espacos_sao_normalizados(self):
        ok, quarentena = validar(venda(id_cliente=" c0002 ", id_produto="p002 "))
        assert quarentena.empty
        assert ok[["id_cliente", "id_produto"]].iloc[0].tolist() == ["C0002", "P002"]

    def test_cadastro_de_referencia_tambem_e_normalizado(self):
        df = pd.DataFrame([venda()])
        df["data_emissao_original"] = df["data_emissao_original"].astype("string")
        ok, _ = validate_vendas(
            df, pd.DataFrame({"id_cliente": [" c0001"]}), pd.DataFrame({"id_produto": ["p001 "]})
        )
        assert len(ok) == 1

    def test_separa_aprovadas_e_rejeitadas_sem_perder_linhas(self):
        ok, quarentena = validar(
            venda(id_venda="V1"), venda(id_venda="V2", qtd_caixas=0), venda(id_venda="V3")
        )
        assert ok["id_venda"].tolist() == ["V1", "V3"]
        assert quarentena["id_venda"].tolist() == ["V2"]

    def test_entrada_vazia(self):
        df = pd.DataFrame(columns=list(venda().keys()))
        ok, quarentena = validate_vendas(df, CLIENTES, PRODUTOS)
        assert ok.empty and quarentena.empty
        assert "motivo_rejeicao" in quarentena.columns
        assert "data_emissao_original" not in ok.columns

    def test_nao_altera_o_dataframe_de_entrada(self):
        df = pd.DataFrame([venda(id_cliente=" c0001 ")])
        df["data_emissao_original"] = df["data_emissao_original"].astype("string")
        validate_vendas(df, CLIENTES, PRODUTOS)
        assert df["id_cliente"].iloc[0] == " c0001 "
