import pandas as pd
import pytest

from pipeline.silver.clean_financeiro import (
    normalize_status,
    normalize_tipo_lancamento,
    parse_competencia,
    validate_financeiro,
)


@pytest.mark.parametrize(
    "origem, esperado",
    [
        # Variações reais de data/raw/financeiro.csv
        ("VENDA", "VENDA"),
        ("venda", "VENDA"),
        ("Venda", "VENDA"),
        ("VENDAS", "VENDA"),
        ("vnd", "VENDA"),
        ("V", "VENDA"),
        ("DEVOLUCAO", "DEVOLUCAO"),
        ("devolução", "DEVOLUCAO"),
        ("Dev", "DEVOLUCAO"),
        ("DEVOL", "DEVOLUCAO"),
        ("D", "DEVOLUCAO"),
        ("BONIFICACAO", "BONIFICACAO"),
        ("bonificação", "BONIFICACAO"),
        ("BONIF.", "BONIFICACAO"),
        ("Bonif", "BONIFICACAO"),
        ("AJUSTE", "AJUSTE"),
        ("outro", "OUTRO"),
        (" venda ", "VENDA"),
    ],
)
def test_normalize_tipo_lancamento(origem, esperado):
    assert normalize_tipo_lancamento(pd.Series([origem])).tolist() == [esperado]


@pytest.mark.parametrize("origem", ["ESTORNO", "", None])
def test_tipo_lancamento_desconhecido_ou_vazio_vira_nulo(origem):
    assert normalize_tipo_lancamento(pd.Series([origem], dtype="object")).isna().all()


@pytest.mark.parametrize(
    "origem, esperado",
    [("PAGO", "PAGO"), (" pago ", "PAGO"), ("Pendente", "PENDENTE"), ("cancelado", "CANCELADO")],
)
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


def lancamentos(*linhas):
    """(id_financeiro, competencia_original, ingerido_em) -> DataFrame como chega à validação."""
    df = pd.DataFrame(linhas, columns=["id_financeiro", "competencia_original", "_ingested_at"])
    df["competencia"] = parse_competencia(df["competencia_original"])
    return df


T1, T2 = pd.Timestamp("2024-06-01"), pd.Timestamp("2024-06-02")


class TestValidateFinanceiro:
    def test_lancamento_valido_passa(self):
        ok, quarentena = validate_financeiro(lancamentos(("F1", "202405", T1), ("F2", "2024/06", T1)))
        assert sorted(ok["id_financeiro"]) == ["F1", "F2"]
        assert quarentena.empty

    def test_competencia_invalida_vai_para_a_quarentena_com_o_valor_original(self):
        ok, quarentena = validate_financeiro(lancamentos(("F1", "202413", T1), ("F2", "202405", T1)))
        assert ok["id_financeiro"].tolist() == ["F2"]
        assert quarentena["id_financeiro"].tolist() == ["F1"]
        assert quarentena["motivo_rejeicao"].tolist() == ["competencia inválida ou ausente: '202413'"]

    def test_id_vazio_ou_nulo_e_rejeitado_antes_de_deduplicar(self):
        """Dois lançamentos sem id não podem virar um só (e sumir da contagem de rejeitados)."""
        df = lancamentos((None, "202405", T1), ("", "202405", T1), ("F1", "202405", T1))
        ok, quarentena = validate_financeiro(df)
        assert ok["id_financeiro"].tolist() == ["F1"]
        assert len(quarentena) == 2
        assert (quarentena["motivo_rejeicao"] == "id_financeiro vazio").all()

    def test_vale_a_versao_mais_recente_do_lancamento(self):
        df = lancamentos(("F1", "202405", T1), ("F1", "202406", T2))
        ok, quarentena = validate_financeiro(df)
        assert len(ok) == 1
        assert ok["competencia"].iloc[0] == pd.Timestamp("2024-06-01")
        assert quarentena.empty

    def test_versao_mais_recente_invalida_nao_ressuscita_a_antiga_valida(self):
        """Mantém o comportamento anterior: deduplica primeiro e só então valida."""
        df = lancamentos(("F1", "202405", T1), ("F1", "202413", T2))
        ok, quarentena = validate_financeiro(df)
        assert ok.empty
        assert quarentena["id_financeiro"].tolist() == ["F1"]

    def test_competencia_ausente(self):
        _, quarentena = validate_financeiro(lancamentos(("F1", None, T1)))
        assert quarentena["motivo_rejeicao"].tolist() == ["competencia inválida ou ausente: 'None'"]
