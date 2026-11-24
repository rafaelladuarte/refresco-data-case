import pandas as pd
import pytest

from pipeline.silver.clean_financeiro import normalize_status, normalize_tipo_lancamento, parse_competencia


@pytest.mark.parametrize(
    "origem, esperado",
    [
        # Variações reais de data/raw/financeiro.csv
        ("VENDA", "VENDA"), ("venda", "VENDA"), ("Venda", "VENDA"), ("VENDAS", "VENDA"), ("vnd", "VENDA"), ("V", "VENDA"),
        ("DEVOLUCAO", "DEVOLUCAO"), ("devolução", "DEVOLUCAO"), ("Dev", "DEVOLUCAO"), ("DEVOL", "DEVOLUCAO"),
        ("D", "DEVOLUCAO"),
        ("BONIFICACAO", "BONIFICACAO"), ("bonificação", "BONIFICACAO"), ("BONIF.", "BONIFICACAO"),
        ("Bonif", "BONIFICACAO"),
        ("AJUSTE", "AJUSTE"), ("outro", "OUTRO"),
        (" venda ", "VENDA"),
    ],
)
def test_normalize_tipo_lancamento(origem, esperado):
    assert normalize_tipo_lancamento(pd.Series([origem])).tolist() == [esperado]


@pytest.mark.parametrize("origem", ["ESTORNO", "", None])
def test_tipo_lancamento_desconhecido_ou_vazio_vira_nulo(origem):
    assert normalize_tipo_lancamento(pd.Series([origem], dtype="object")).isna().all()


@pytest.mark.parametrize("origem, esperado", [("PAGO", "PAGO"), (" pago ", "PAGO"), ("Pendente", "PENDENTE"),
                                              ("cancelado", "CANCELADO")])
def test_normalize_status(origem, esperado):
    assert normalize_status(pd.Series([origem])).tolist() == [esperado]


@pytest.mark.parametrize("origem", ["QUITADO", "", None])
def test_status_desconhecido_ou_vazio_vira_nulo(origem):
    assert normalize_status(pd.Series([origem], dtype="object")).isna().all()


class TestParseCompetencia:
    @pytest.mark.parametrize("origem", ["202407", "2024/07", " 202407 ", 202407])
    def test_formatos_aceitos(self, origem):
        assert parse_competencia(pd.Series([origem]))[0] == pd.Timestamp("2024-07-01")

    @pytest.mark.parametrize("origem", ["202413", "07/2024", "abc", "", None])
    def test_invalida_vira_nat(self, origem):
        assert pd.isna(parse_competencia(pd.Series([origem], dtype="object"))[0])
