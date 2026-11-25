"""Ingestão de clima com respostas HTTP simuladas (sem rede)."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from pipeline.bronze import ingest_clima
from pipeline.common.config import ClimaConfig

CONFIG = ClimaConfig(
    api_url="http://api.fake/archive", latitude=-18.9, longitude=-48.3, ano_inicio=2023, ano_fim=2024
)

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
        with (
            patch.object(ingest_clima.requests, "get", return_value=resposta(status=404)),
            pytest.raises(requests.HTTPError),
        ):
            ingest_clima.fetch_weather_data_by_year(CONFIG, 2024)

    @pytest.mark.parametrize("corpo", [{"reason": "x"}, {"daily": {}}])
    def test_corpo_sem_daily_gera_erro(self, corpo):
        with (
            patch.object(ingest_clima.requests, "get", return_value=resposta(corpo=corpo)),
            pytest.raises(ValueError, match="daily"),
        ):
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
        with (
            patch.object(ingest_clima.requests, "get", return_value=resposta(status=503)),
            pytest.raises(RuntimeError, match="Nenhum dado de clima"),
        ):
            ingest_clima.fetch_all_weather_data(CONFIG)

    def test_concatena_todos_os_anos(self):
        with patch.object(ingest_clima.requests, "get", side_effect=[resposta(), resposta()]):
            df = ingest_clima.fetch_all_weather_data(CONFIG)
        assert df["year"].tolist() == [2023, 2023, 2024, 2024]
        assert df.index.tolist() == [0, 1, 2, 3]


class TestFixtureDir:
    """CLIMA_FIXTURE_DIR: lê respostas gravadas em vez de chamar a API (job e2e da CI)."""

    def _config(self, tmp_path):
        return ClimaConfig(
            api_url="http://api.fake/archive",
            latitude=-18.9,
            longitude=-48.3,
            ano_inicio=2023,
            ano_fim=2024,
            fixture_dir=str(tmp_path),
        )

    def test_le_arquivo_do_ano_sem_chamar_a_rede(self, tmp_path):
        (tmp_path / "2024.json").write_text(json.dumps({"daily": DAILY}), encoding="utf-8")

        with patch.object(ingest_clima.requests, "get") as get:
            df = ingest_clima.fetch_weather_data_by_year(self._config(tmp_path), 2024)

        get.assert_not_called()
        assert df["time"].tolist() == DAILY["time"]
        assert (df["year"] == 2024).all()

    def test_fixture_ausente_vira_valueerror_e_ano_e_pulado(self, tmp_path):
        (tmp_path / "2024.json").write_text(json.dumps({"daily": DAILY}), encoding="utf-8")

        df = ingest_clima.fetch_all_weather_data(self._config(tmp_path))

        assert df["year"].unique().tolist() == [2024]

    def test_fixture_sem_bloco_daily_falha(self, tmp_path):
        (tmp_path / "2024.json").write_text("{}", encoding="utf-8")

        with pytest.raises(ValueError, match="daily"):
            ingest_clima.fetch_weather_data_by_year(self._config(tmp_path), 2024)

    def test_fixtures_versionadas_cobrem_2020_a_2025(self):
        pasta = Path(__file__).parents[1] / "fixtures" / "open_meteo"
        config = ClimaConfig(
            api_url="x", latitude=0, longitude=0, ano_inicio=2020, ano_fim=2025, fixture_dir=str(pasta)
        )

        df = ingest_clima.fetch_all_weather_data(config)

        assert sorted(df["year"].unique()) == [2020, 2021, 2022, 2023, 2024, 2025]
        assert len(df) == 2192
        assert df["temperature_2m_max"].notna().all()
