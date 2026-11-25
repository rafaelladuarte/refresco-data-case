import pandas as pd

from pipeline.silver.clean_clima import validate_clima


def clima(*datas):
    return pd.DataFrame(
        {"time": pd.to_datetime(list(datas), errors="coerce"), "temperature_2m_max": range(len(datas))}
    )


def test_medicao_com_data_valida_passa():
    ok, quarentena = validate_clima(clima("2024-01-01", "2024-01-02"))
    assert len(ok) == 2
    assert quarentena.empty
    assert "motivo_rejeicao" in quarentena.columns


def test_medicao_sem_data_vai_para_a_quarentena_com_motivo():
    ok, quarentena = validate_clima(clima("2024-01-01", None, "data-ruim"))
    assert ok["time"].dt.strftime("%Y-%m-%d").tolist() == ["2024-01-01"]
    assert len(quarentena) == 2
    assert (quarentena["motivo_rejeicao"] == "time inválido ou ausente").all()
