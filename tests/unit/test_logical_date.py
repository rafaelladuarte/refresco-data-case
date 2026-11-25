from datetime import date

import pytest

from pipeline.common.logical_date import resolve_ds


def test_sem_valor_usa_hoje():
    assert resolve_ds(None) == date.today().isoformat()


def test_data_valida_e_devolvida_como_veio():
    assert resolve_ds("2024-02-29") == "2024-02-29"


@pytest.mark.parametrize("invalida", ["", "2024-2-9", "09/02/2024", "2024-02-30", "2024-13-01", "ontem"])
def test_data_invalida_gera_erro(invalida):
    with pytest.raises(ValueError, match="Data lógica inválida"):
        resolve_ds(invalida)
