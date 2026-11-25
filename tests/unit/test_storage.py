"""Testes de pipeline.common.storage com um filesystem falso (sem MinIO)."""

from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest
from fsspec.implementations.memory import MemoryFileSystem

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
        fs.glob.return_value = [
            "bronze/vendas/ingestion_date=1/v.csv",
            "bronze/vendas/ingestion_date=2/v.csv",
        ]
        partes = [pd.DataFrame({"id": ["A"]}), pd.DataFrame({"id": ["B", "C"]})]
        with patch.object(storage.pd, "read_csv", side_effect=partes) as read_csv:
            df = storage.read_bronze(fs, "vendas")
        assert df["id"].tolist() == ["A", "B", "C"]
        assert df.index.tolist() == [0, 1, 2]
        read_csv.assert_has_calls(
            [
                call("s3://bronze/vendas/ingestion_date=1/v.csv", storage_options=OPTS),
                call("s3://bronze/vendas/ingestion_date=2/v.csv", storage_options=OPTS),
            ]
        )


def test_read_silver_usa_as_credenciais():
    with patch.object(storage.pd, "read_parquet", return_value=pd.DataFrame()) as read_parquet:
        storage.read_silver("s3://silver/clientes/")
    read_parquet.assert_called_once_with("s3://silver/clientes/", storage_options=OPTS)


class S3LikeMemoryFS(MemoryFileSystem):
    """Filesystem em memória com caminhos como os do S3 (`bucket/chave`, sem `/` inicial).

    O MemoryFileSystem padrão devolve `/bucket/chave`, e o pyarrow rejeita a divergência.
    """

    protocol = "s3like"
    store: dict = {}
    pseudo_dirs: list = [""]

    @classmethod
    def _strip_protocol(cls, path):
        return super()._strip_protocol(path).lstrip("/")


@pytest.fixture(autouse=True)
def _sem_espera_entre_tentativas():
    with patch.object(storage, "WRITE_RETRY_DELAY_SECONDS", 0):
        yield


@pytest.fixture
def memfs():
    """Escrita atômica testada de verdade, sem mocks de filesystem."""
    fs = S3LikeMemoryFS()
    fs.store.clear()
    fs.pseudo_dirs[:] = [""]
    yield fs
    fs.store.clear()
    fs.pseudo_dirs[:] = [""]


def df_v(*ids):
    return pd.DataFrame({"id": list(ids), "ano": [2024] * len(ids), "mes": [1] * len(ids)})


def ler(fs, path):
    return sorted(pd.read_parquet(path, filesystem=fs)["id"].tolist())


def sobras(fs, bucket="silver"):
    """Tudo que a escrita atômica deixaria para trás (_tmp/ e _prev/)."""
    return [p for p in fs.find(bucket) if "/_tmp/" in p or "/_prev/" in p]


class TestWriteSilver:
    def test_primeira_escrita_cria_o_dataset_particionado(self, memfs):
        storage.write_silver(memfs, df_v("a", "b"), "s3://silver/vendas/", partition_cols=["ano", "mes"])
        assert ler(memfs, "silver/vendas") == ["a", "b"]
        assert any("ano=2024/mes=1" in p for p in memfs.find("silver/vendas"))
        assert sobras(memfs) == []

    def test_substitui_a_versao_anterior(self, memfs):
        storage.write_silver(memfs, df_v("a", "b"), "s3://silver/vendas/", ["ano", "mes"])
        storage.write_silver(memfs, df_v("c"), "s3://silver/vendas/", ["ano", "mes"])
        assert ler(memfs, "silver/vendas") == ["c"]
        assert sobras(memfs) == []

    def test_arquivo_unico_sem_particao(self, memfs):
        storage.write_silver(memfs, df_v("a"), "s3://silver/produtos/produtos_clean.parquet")
        storage.write_silver(memfs, df_v("b", "c"), "s3://silver/produtos/produtos_clean.parquet")
        assert ler(memfs, "silver/produtos/produtos_clean.parquet") == ["b", "c"]
        assert sobras(memfs) == []

    def test_falha_na_gravacao_preserva_a_versao_anterior(self, memfs):
        storage.write_silver(memfs, df_v("a", "b"), "s3://silver/vendas/", ["ano", "mes"])
        with (
            patch.object(pd.DataFrame, "to_parquet", side_effect=OSError("MinIO caiu")),
            pytest.raises(OSError, match="MinIO caiu"),
        ):
            storage.write_silver(memfs, df_v("c"), "s3://silver/vendas/", ["ano", "mes"])
        assert ler(memfs, "silver/vendas") == ["a", "b"]
        assert sobras(memfs) == []

    def test_erro_transitorio_na_gravacao_e_refeito(self, memfs):
        storage.write_silver(memfs, df_v("a", "b"), "s3://silver/vendas/", ["ano", "mes"])
        original = pd.DataFrame.to_parquet
        chamadas = []

        def falha_na_primeira(self, *args, **kwargs):
            chamadas.append(1)
            if len(chamadas) == 1:
                raise OSError("ClientDisconnected")
            return original(self, *args, **kwargs)

        with patch.object(pd.DataFrame, "to_parquet", falha_na_primeira):
            storage.write_silver(memfs, df_v("c"), "s3://silver/vendas/", ["ano", "mes"])
        assert len(chamadas) == 2
        assert ler(memfs, "silver/vendas") == ["c"]
        assert sobras(memfs) == []

    def test_falha_persistente_esgota_as_tentativas_e_preserva_a_versao(self, memfs):
        storage.write_silver(memfs, df_v("a"), "s3://silver/vendas/", ["ano", "mes"])
        with (
            patch.object(pd.DataFrame, "to_parquet", side_effect=OSError("MinIO fora do ar")) as grava,
            pytest.raises(OSError, match="MinIO fora do ar"),
        ):
            storage.write_silver(memfs, df_v("b"), "s3://silver/vendas/", ["ano", "mes"])
        assert grava.call_count == storage.WRITE_ATTEMPTS
        assert ler(memfs, "silver/vendas") == ["a"]

    def test_gravacao_truncada_e_barrada_pela_validacao(self, memfs):
        storage.write_silver(memfs, df_v("a", "b"), "s3://silver/vendas/", ["ano", "mes"])
        original = pd.DataFrame.to_parquet

        def grava_so_a_primeira_linha(self, *args, **kwargs):
            return original(self.head(1), *args, **kwargs)

        with (
            patch.object(pd.DataFrame, "to_parquet", grava_so_a_primeira_linha),
            pytest.raises(RuntimeError, match="Validação da escrita falhou"),
        ):
            storage.write_silver(memfs, df_v("c", "d", "e"), "s3://silver/vendas/", ["ano", "mes"])
        assert ler(memfs, "silver/vendas") == ["a", "b"]
        assert sobras(memfs) == []

    def test_falha_ao_publicar_restaura_a_versao_anterior(self, memfs):
        storage.write_silver(memfs, df_v("a", "b"), "s3://silver/vendas/", ["ano", "mes"])
        mv_original = memfs.mv
        chamadas = []

        def mv_que_falha_ao_publicar(origem, destino, **kw):
            chamadas.append((origem, destino))
            # 1ª chamada: move a versão atual para _prev (ok). 2ª: publica a nova (falha).
            if len(chamadas) == 2:
                raise OSError("falha ao publicar")
            return mv_original(origem, destino, **kw)

        with (
            patch.object(memfs, "mv", mv_que_falha_ao_publicar),
            pytest.raises(OSError, match="falha ao publicar"),
        ):
            storage.write_silver(memfs, df_v("c"), "s3://silver/vendas/", ["ano", "mes"])
        assert ler(memfs, "silver/vendas") == ["a", "b"]
        assert sobras(memfs) == []

    def test_queda_no_meio_da_troca_e_recuperada_na_proxima_escrita(self, memfs):
        """Simula o processo morrendo entre mover a versão antiga e publicar a nova."""
        storage.write_silver(memfs, df_v("a", "b"), "s3://silver/vendas/", ["ano", "mes"])
        memfs.mv("silver/vendas", "silver/_prev/vendas", recursive=True)  # o que sobra após a queda
        assert not memfs.exists("silver/vendas")

        storage.write_silver(memfs, df_v("c"), "s3://silver/vendas/", ["ano", "mes"])

        assert ler(memfs, "silver/vendas") == ["c"]
        assert sobras(memfs) == []

    def test_queda_na_troca_com_nova_escrita_falhando_mantem_a_versao_restaurada(self, memfs):
        storage.write_silver(memfs, df_v("a", "b"), "s3://silver/vendas/", ["ano", "mes"])
        memfs.mv("silver/vendas", "silver/_prev/vendas", recursive=True)
        with (
            patch.object(pd.DataFrame, "to_parquet", side_effect=OSError("MinIO caiu")),
            pytest.raises(OSError),
        ):
            storage.write_silver(memfs, df_v("c"), "s3://silver/vendas/", ["ano", "mes"])
        assert ler(memfs, "silver/vendas") == ["a", "b"]

    def test_prev_sobrando_depois_de_troca_concluida_e_removido(self, memfs):
        storage.write_silver(memfs, df_v("a"), "s3://silver/vendas/", ["ano", "mes"])
        memfs.pipe("silver/_prev/vendas/velho.parquet", b"x")
        storage.write_silver(memfs, df_v("b"), "s3://silver/vendas/", ["ano", "mes"])
        assert ler(memfs, "silver/vendas") == ["b"]
        assert sobras(memfs) == []

    def test_tmp_orfao_de_outra_execucao_e_removido_sem_tocar_nas_tasks_irmas(self, memfs):
        memfs.pipe("silver/_tmp/execucao-antiga/vendas/lixo.parquet", b"x")
        memfs.pipe("silver/_tmp/execucao-atual/clientes/em_andamento.parquet", b"x")
        storage.write_silver(memfs, df_v("a"), "s3://silver/vendas/", ["ano", "mes"])
        assert not memfs.exists("silver/_tmp/execucao-antiga/vendas")
        assert memfs.exists("silver/_tmp/execucao-atual/clientes/em_andamento.parquet")

    def test_datasets_diferentes_nao_interferem_entre_si(self, memfs):
        storage.write_silver(memfs, df_v("a"), "s3://silver/clientes/", ["ano", "mes"])
        storage.write_silver(memfs, df_v("b"), "s3://silver/vendas/", ["ano", "mes"])
        assert ler(memfs, "silver/clientes") == ["a"]
        assert ler(memfs, "silver/vendas") == ["b"]


class TestWriteQuarantine:
    def test_grava_rejeitados_e_retorna_o_caminho(self, memfs):
        path = storage.write_quarantine(
            memfs, pd.DataFrame({"id": ["x"], "motivo_rejeicao": ["m"]}), "vendas"
        )
        assert path == "s3://silver/_quarentena/vendas/"
        assert ler(memfs, "silver/_quarentena/vendas") == ["x"]
        assert sobras(memfs) == []

    def test_substitui_a_quarentena_anterior(self, memfs):
        storage.write_quarantine(memfs, pd.DataFrame({"id": ["x", "y"]}), "vendas")
        storage.write_quarantine(memfs, pd.DataFrame({"id": ["z"]}), "vendas")
        assert ler(memfs, "silver/_quarentena/vendas") == ["z"]

    def test_quarentena_vazia_remove_a_anterior_e_nao_grava(self, memfs):
        storage.write_quarantine(memfs, pd.DataFrame({"id": ["x"]}), "clientes")
        storage.write_quarantine(memfs, pd.DataFrame({"id": []}), "clientes")
        # no S3 diretório sem objetos não existe; o filesystem em memória guarda o diretório vazio
        assert memfs.find("silver/_quarentena/clientes") == []

    def test_quarentena_vazia_sem_anterior_nao_faz_nada(self, memfs):
        storage.write_quarantine(memfs, pd.DataFrame({"id": []}), "clientes")
        assert memfs.find("silver") == []

    def test_falha_preserva_a_quarentena_anterior(self, memfs):
        storage.write_quarantine(memfs, pd.DataFrame({"id": ["x"]}), "vendas")
        with (
            patch.object(pd.DataFrame, "to_parquet", side_effect=OSError("MinIO caiu")),
            pytest.raises(OSError),
        ):
            storage.write_quarantine(memfs, pd.DataFrame({"id": ["novo"]}), "vendas")
        assert ler(memfs, "silver/_quarentena/vendas") == ["x"]
