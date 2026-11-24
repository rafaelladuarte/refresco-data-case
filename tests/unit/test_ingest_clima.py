"""Ingestão de clima com respostas HTTP simuladas (sem rede)."""
from unittest.mock import MagicMock, patch

import pytest
import requests

from pipeline.bronze import ingest_clima
from pipeline.common.config import ClimaConfig

CONFIG = ClimaConfig(api_url="http://api.fake/archive", latitude=-18.9, longitude=-48.3, ano_inicio=2023,
                     ano_fim=2024)

DAILY = {
    "time": ["2024-01-01", "2024-01-02"],
    "temperature_2m_max": [30.1, 28.4],
    "precipitation_sum": [0.0, 12.5],
}


def resposta(status=200, corpo=None):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = corpo if corpo is not None else {"daily": DAILY}
    if status >= 400:
        r.raise_for_status.side_effect = requests.HTTPError(f"{status} Client Error")
    return r


class TestFetchWeatherDataByYear:
    def test_sucesso_monta_dataframe_e_parametros(self):
        with patch.object(ingest_clima.requests, "get", return_value=resposta()) as get:
            df = ingest_clima.fetch_weather_data_by_year(CONFIG, 2024)

        assert df["time"].tolist() == DAILY["time"]
        assert (df["year"] == 2024).all()
        _, kwargs = get.call_args
        assert get.call_args.args == ("http://api.fake/archive",)
        assert kwargs["params"]["start_date"] == "2024-01-01"
        assert kwargs["params"]["end_date"] == "2024-12-31"
        assert kwargs["params"]["latitude"] == -18.9
        assert kwargs["timeout"] == ingest_clima.REQUEST_TIMEOUT_SECONDS

    def test_http_404_propaga_erro(self):
        with patch.object(ingest_clima.requests, "get", return_value=resposta(status=404)):
            with pytest.raises(requests.HTTPError):
                ingest_clima.fetch_weather_data_by_year(CONFIG, 2024)

    @pytest.mark.parametrize("corpo", [{"reason": "x"}, {"daily": {}}])
    def test_corpo_sem_daily_gera_erro(self, corpo):
        with patch.object(ingest_clima.requests, "get", return_value=resposta(corpo=corpo)):
            with pytest.raises(ValueError, match="daily"):
                ingest_clima.fetch_weather_data_by_year(CONFIG, 2024)


class TestFetchAllWeatherData:
    def test_falha_em_um_ano_nao_interrompe_os_demais(self):
        with patch.object(ingest_clima.requests, "get", side_effect=[resposta(status=500), resposta()]):
            df = ingest_clima.fetch_all_weather_data(CONFIG)
        assert df["year"].unique().tolist() == [2024]

    def test_erro_de_rede_e_tratado_como_falha_do_ano(self):
        with patch.object(ingest_clima.requests, "get", side_effect=[requests.ConnectionError(), resposta()]):
            assert len(ingest_clima.fetch_all_weather_data(CONFIG)) == 2

    def test_todos_os_anos_falhando_gera_erro(self):
        with patch.object(ingest_clima.requests, "get", return_value=resposta(status=503)):
            with pytest.raises(RuntimeError, match="Nenhum dado de clima"):
                ingest_clima.fetch_all_weather_data(CONFIG)

    def test_concatena_todos_os_anos(self):
        with patch.object(ingest_clima.requests, "get", side_effect=[resposta(), resposta()]):
            df = ingest_clima.fetch_all_weather_data(CONFIG)
        assert df["year"].tolist() == [2023, 2023, 2024, 2024]
        assert df.index.tolist() == [0, 1, 2, 3]
