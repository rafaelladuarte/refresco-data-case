"""Ingestão CSV -> Bronze: partição por data lógica e idempotência (sem MinIO)."""

import pandas as pd
import pytest

from pipeline.bronze import ingest_csv
from tests.unit.test_storage import S3LikeMemoryFS


@pytest.fixture
def memfs():
    fs = S3LikeMemoryFS()
    fs.store.clear()
    fs.pseudo_dirs[:] = [""]
    yield fs
    fs.store.clear()
    fs.pseudo_dirs[:] = [""]


@pytest.fixture
def raw(tmp_path, monkeypatch):
    (tmp_path / "vendas.csv").write_text("id_venda,qtd\nV1,10\nV2,20\n", encoding="utf-8")
    monkeypatch.setattr(ingest_csv, "LOCAL_RAW_DIR", tmp_path)
    return tmp_path


def ler(fs, ds):
    with fs.open(f"bronze/vendas/ingestion_date={ds}/vendas.csv", "rb") as f:
        return pd.read_csv(f)


def test_grava_na_particao_da_data_logica_com_a_coluna_de_controle(memfs, raw):
    ingest_csv.process_table(memfs, "vendas", "2024-06-01")

    df = ler(memfs, "2024-06-01")
    assert df["id_venda"].tolist() == ["V1", "V2"]
    assert (df["_logical_date"] == "2024-06-01").all()
    assert df["_ingested_at"].notna().all()
    assert (df["_source_file"] == "vendas.csv").all()


def test_reexecutar_a_mesma_data_sobrescreve_a_mesma_particao(memfs, raw):
    ingest_csv.process_table(memfs, "vendas", "2024-06-01")
    (raw / "vendas.csv").write_text("id_venda,qtd\nV1,99\n", encoding="utf-8")

    ingest_csv.process_table(memfs, "vendas", "2024-06-01")

    assert ler(memfs, "2024-06-01")["qtd"].tolist() == [99]
    assert memfs.glob("bronze/vendas/*/vendas.csv") == ["bronze/vendas/ingestion_date=2024-06-01/vendas.csv"]


def test_datas_diferentes_geram_particoes_diferentes(memfs, raw):
    ingest_csv.process_table(memfs, "vendas", "2024-06-01")
    ingest_csv.process_table(memfs, "vendas", "2024-06-02")

    assert len(memfs.glob("bronze/vendas/*/vendas.csv")) == 2
    assert (ler(memfs, "2024-06-02")["_logical_date"] == "2024-06-02").all()


def test_main_valida_a_data_logica(raw):
    with pytest.raises(ValueError, match="Data lógica inválida"):
        ingest_csv.main("01/06/2024")
