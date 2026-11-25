import pytest

from pipeline.common.config import (
    DEFAULT_OPEN_METEO_URL,
    DEFAULT_S3_ENDPOINT,
    ClimaConfig,
    PostgresConfig,
    S3Config,
    postgres_url,
    require_env,
    s3_storage_options,
)

CLIMA_VARS = [
    "OPEN_METEO_ARCHIVE_URL",
    "CLIMA_LATITUDE",
    "CLIMA_LONGITUDE",
    "CLIMA_ANO_INICIO",
    "CLIMA_ANO_FIM",
    "CLIMA_FIXTURE_DIR",
]


class TestRequireEnv:
    def test_retorna_valor(self, monkeypatch):
        monkeypatch.setenv("X_TESTE", "valor")
        assert require_env("X_TESTE") == "valor"

    @pytest.mark.parametrize("valor", [None, ""])
    def test_ausente_ou_vazia_falha_com_o_nome_da_variavel(self, monkeypatch, valor):
        if valor is None:
            monkeypatch.delenv("X_TESTE", raising=False)
        else:
            monkeypatch.setenv("X_TESTE", valor)
        with pytest.raises(RuntimeError, match="X_TESTE"):
            require_env("X_TESTE")


class TestS3Config:
    def test_storage_options_com_endpoint_padrao(self, monkeypatch):
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "k")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "s")
        monkeypatch.delenv("AIRFLOW__S3__ENDPOINT_URL", raising=False)
        assert s3_storage_options() == {
            "key": "k",
            "secret": "s",
            "client_kwargs": {"endpoint_url": DEFAULT_S3_ENDPOINT},
        }

    def test_endpoint_do_ambiente(self, monkeypatch):
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "k")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "s")
        monkeypatch.setenv("AIRFLOW__S3__ENDPOINT_URL", "http://minio:9000")
        assert S3Config.from_env().endpoint_url == "http://minio:9000"

    def test_credencial_ausente_falha(self, monkeypatch):
        monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "s")
        with pytest.raises(RuntimeError, match="AWS_ACCESS_KEY_ID"):
            S3Config.from_env()


class TestPostgresConfig:
    def test_url_com_host_e_porta_padrao(self, monkeypatch):
        for k, v in {"POSTGRES_USER": "u", "POSTGRES_PASSWORD": "p", "POSTGRES_DB": "gold"}.items():
            monkeypatch.setenv(k, v)
        monkeypatch.delenv("POSTGRES_HOST", raising=False)
        monkeypatch.delenv("POSTGRES_PORT", raising=False)
        assert postgres_url() == "postgresql://u:p@localhost:5432/gold"

    def test_senha_ausente_falha(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_USER", "u")
        monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
        with pytest.raises(RuntimeError, match="POSTGRES_PASSWORD"):
            PostgresConfig.from_env()


class TestClimaConfig:
    @pytest.fixture(autouse=True)
    def _limpa_ambiente(self, monkeypatch):
        for var in CLIMA_VARS:
            monkeypatch.delenv(var, raising=False)

    def test_padrao_uberlandia_2020_2025(self):
        config = ClimaConfig.from_env()
        assert config == ClimaConfig(
            api_url=DEFAULT_OPEN_METEO_URL,
            latitude=-18.9186,
            longitude=-48.2772,
            ano_inicio=2020,
            ano_fim=2025,
        )

    def test_valores_do_ambiente_sao_tipados(self, monkeypatch):
        monkeypatch.setenv("CLIMA_LATITUDE", "-23.5")
        monkeypatch.setenv("CLIMA_ANO_INICIO", "2024")
        monkeypatch.setenv("CLIMA_ANO_FIM", "2024")
        config = ClimaConfig.from_env()
        assert (config.latitude, config.ano_inicio, config.ano_fim) == (-23.5, 2024, 2024)

    def test_fixture_dir_vem_do_ambiente_e_vazio_vira_none(self, monkeypatch):
        assert ClimaConfig.from_env().fixture_dir is None
        monkeypatch.setenv("CLIMA_FIXTURE_DIR", "tests/fixtures/open_meteo")
        assert ClimaConfig.from_env().fixture_dir == "tests/fixtures/open_meteo"
        monkeypatch.setenv("CLIMA_FIXTURE_DIR", "")
        assert ClimaConfig.from_env().fixture_dir is None

    def test_periodo_invertido_gera_erro(self, monkeypatch):
        monkeypatch.setenv("CLIMA_ANO_INICIO", "2025")
        monkeypatch.setenv("CLIMA_ANO_FIM", "2020")
        with pytest.raises(ValueError, match="maior que"):
            ClimaConfig.from_env()
