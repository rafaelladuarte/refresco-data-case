import pandas as pd
import pytest

from pipeline.common.validators import (
    PATTERN_ID_CLIENTE,
    PATTERN_ID_PRODUTO,
    clean_text,
    normalize_id,
)


class TestCleanText:
    def test_nulo_continua_nulo_e_nunca_vira_string_nan(self):
        result = clean_text(pd.Series([None, float("nan"), pd.NA]))
        assert result.isna().all()
        assert "nan" not in result.dropna().tolist()

    def test_vazio_e_so_espacos_viram_nulo(self):
        assert clean_text(pd.Series(["", "   "])).isna().all()

    def test_remove_espacos_e_preserva_caixa_e_acentos(self):
        assert clean_text(pd.Series(["  Conveniência ", "SP"])).tolist() == ["Conveniência", "SP"]

    def test_retorna_dtype_string_nullable(self):
        assert clean_text(pd.Series(["a", None])).dtype == "string"


class TestNormalizeId:
    def test_maiusculas_e_sem_espacos(self):
        assert normalize_id(pd.Series([" c0001 ", "p001"])).tolist() == ["C0001", "P001"]

    def test_nulo_vira_string_vazia_e_nunca_nan(self):
        assert normalize_id(pd.Series([None, float("nan")])).tolist() == ["", ""]


@pytest.mark.parametrize(
    "pattern, validos, invalidos",
    [
        (PATTERN_ID_CLIENTE, ["C0001", "c9999"], ["C001", "C00001", "X0001", "C000A", "", " C0001"]),
        (PATTERN_ID_PRODUTO, ["P001", "p999"], ["P01", "P0001", "C001", "P00A", ""]),
    ],
)
def test_patterns_de_id(pattern, validos, invalidos):
    assert all(pattern.match(v) for v in validos)
    assert not any(pattern.match(v) for v in invalidos)
