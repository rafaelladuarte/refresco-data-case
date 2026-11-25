"""scripts/cleanup_legacy_bronze.py: só apaga partições no formato antigo, e só com --apply."""

from unittest.mock import patch

import cleanup_legacy_bronze as cl
import pytest

from tests.unit.test_storage import S3LikeMemoryFS


@pytest.fixture
def memfs():
    fs = S3LikeMemoryFS()
    fs.store.clear()
    fs.pseudo_dirs[:] = [""]
    for caminho in [
        "bronze/vendas/ingestion_date=2024-06-01/vendas.csv",
        "bronze/vendas/ingestion_date=2026-11-25T12:17:55/vendas.csv",
        "bronze/clima/ingestion_date=2026-11-25T12:17:56/clima.csv",
        "bronze/clientes/ingestion_date=2024-06-01/clientes.csv",
    ]:
        fs.pipe(caminho, b"x")
    yield fs
    fs.store.clear()
    fs.pseudo_dirs[:] = [""]


def test_lista_apenas_as_particoes_no_formato_antigo(memfs):
    assert cl.particoes_antigas(memfs) == [
        "bronze/clima/ingestion_date=2026-11-25T12:17:56",
        "bronze/vendas/ingestion_date=2026-11-25T12:17:55",
    ]


def test_dry_run_nao_apaga_nada(memfs, capsys):
    with patch.object(cl, "get_s3_client", return_value=memfs):
        assert cl.main([]) == 0
    assert len(memfs.find("bronze")) == 4
    assert "Rode com --apply" in capsys.readouterr().out


def test_apply_apaga_so_as_antigas(memfs):
    with patch.object(cl, "get_s3_client", return_value=memfs):
        assert cl.main(["--apply"]) == 0
    assert sorted(memfs.find("bronze")) == [
        "bronze/clientes/ingestion_date=2024-06-01/clientes.csv",
        "bronze/vendas/ingestion_date=2024-06-01/vendas.csv",
    ]


def test_sem_particoes_antigas(memfs, capsys):
    memfs.rm("bronze/vendas/ingestion_date=2026-11-25T12:17:55", recursive=True)
    memfs.rm("bronze/clima/ingestion_date=2026-11-25T12:17:56", recursive=True)
    with patch.object(cl, "get_s3_client", return_value=memfs):
        assert cl.main(["--apply"]) == 0
    assert "Nenhuma partição antiga" in capsys.readouterr().out
