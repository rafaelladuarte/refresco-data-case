"""Testes de pipeline.common.storage com um filesystem falso (sem MinIO)."""
from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest

from pipeline.common import storage

OPTS = {"key": "k", "secret": "s", "client_kwargs": {"endpoint_url": "http://fake"}}


@pytest.fixture(autouse=True)
def _storage_options():
    with patch.object(storage, "s3_storage_options", return_value=OPTS):
        yield


@pytest.fixture
def fs():
    return MagicMock()


def test_get_s3_client_usa_as_credenciais():
    with patch.object(storage.s3fs, "S3FileSystem") as s3:
        storage.get_s3_client()
    s3.assert_called_once_with(**OPTS)


class TestEnsureBucket:
    def test_cria_bucket_inexistente(self, fs):
        fs.exists.return_value = False
        assert storage.ensure_bucket(fs, "bronze") is True
        fs.mkdir.assert_called_once_with("bronze")

    def test_bucket_existente_nao_e_recriado(self, fs):
        fs.exists.return_value = True
        assert storage.ensure_bucket(fs, "bronze") is True
        fs.mkdir.assert_not_called()

    def test_erro_retorna_false(self, fs):
        fs.exists.side_effect = OSError("sem conexão")
        assert storage.ensure_bucket(fs, "bronze") is False


class TestReadBronze:
    def test_sem_arquivos_retorna_none(self, fs):
        fs.glob.return_value = []
        assert storage.read_bronze(fs, "vendas") is None
        fs.glob.assert_called_once_with("bronze/vendas/**/*.csv")

    def test_concatena_todas_as_particoes_com_indice_novo(self, fs):
        fs.glob.return_value = ["bronze/vendas/ingestion_date=1/v.csv", "bronze/vendas/ingestion_date=2/v.csv"]
        partes = [pd.DataFrame({"id": ["A"]}), pd.DataFrame({"id": ["B", "C"]})]
        with patch.object(storage.pd, "read_csv", side_effect=partes) as read_csv:
            df = storage.read_bronze(fs, "vendas")
        assert df["id"].tolist() == ["A", "B", "C"]
        assert df.index.tolist() == [0, 1, 2]
        read_csv.assert_has_calls([
            call("s3://bronze/vendas/ingestion_date=1/v.csv", storage_options=OPTS),
            call("s3://bronze/vendas/ingestion_date=2/v.csv", storage_options=OPTS),
        ])


def test_read_silver_usa_as_credenciais():
    with patch.object(storage.pd, "read_parquet", return_value=pd.DataFrame()) as read_parquet:
        storage.read_silver("s3://silver/clientes/")
    read_parquet.assert_called_once_with("s3://silver/clientes/", storage_options=OPTS)


class TestWriteSilver:
    def test_remove_conteudo_anterior_antes_de_gravar(self, fs):
        fs.exists.return_value = True
        df = pd.DataFrame({"a": [1]})
        with patch.object(pd.DataFrame, "to_parquet") as to_parquet:
            storage.write_silver(fs, df, "s3://silver/vendas/", partition_cols=["ano", "mes"])
        fs.rm.assert_called_once_with("s3://silver/vendas/", recursive=True)
        to_parquet.assert_called_once_with(
            path="s3://silver/vendas/", partition_cols=["ano", "mes"],
            storage_options=OPTS, index=False, compression="snappy",
        )

    def test_primeira_escrita_nao_remove_nada(self, fs):
        fs.exists.return_value = False
        with patch.object(pd.DataFrame, "to_parquet"):
            storage.write_silver(fs, pd.DataFrame({"a": [1]}), "s3://silver/produtos/p.parquet")
        fs.rm.assert_not_called()


class TestWriteQuarantine:
    def test_grava_rejeitados_e_retorna_o_caminho(self, fs):
        fs.exists.return_value = True
        with patch.object(pd.DataFrame, "to_parquet") as to_parquet:
            path = storage.write_quarantine(fs, pd.DataFrame({"motivo_rejeicao": ["x"]}), "vendas")
        assert path == "s3://silver/_quarentena/vendas/"
        fs.rm.assert_called_once_with(path, recursive=True)
        to_parquet.assert_called_once()

    def test_primeira_quarentena_nao_remove_nada(self, fs):
        fs.exists.return_value = False
        with patch.object(pd.DataFrame, "to_parquet") as to_parquet:
            storage.write_quarantine(fs, pd.DataFrame({"motivo_rejeicao": ["x"]}), "vendas")
        fs.rm.assert_not_called()
        to_parquet.assert_called_once()

    def test_quarentena_vazia_remove_a_anterior_e_nao_grava(self, fs):
        fs.exists.return_value = True
        with patch.object(pd.DataFrame, "to_parquet") as to_parquet:
            storage.write_quarantine(fs, pd.DataFrame({"motivo_rejeicao": []}), "clientes")
        fs.rm.assert_called_once_with("s3://silver/_quarentena/clientes/", recursive=True)
        to_parquet.assert_not_called()
