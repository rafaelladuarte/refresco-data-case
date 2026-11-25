"""Merge incremental da Silver: precedência por data lógica, idempotência e _changed_at estável."""

import pandas as pd
import pytest

from pipeline.common import merge

KEY = ["id"]
T1 = pd.Timestamp("2026-01-01 10:00:00")
T2 = pd.Timestamp("2026-01-02 10:00:00")
T3 = pd.Timestamp("2026-01-03 10:00:00")


def lote(ds, ingerido_em, **linhas):
    """Partição Bronze já limpa: `linhas` é {id: valor}."""
    return pd.DataFrame(
        {
            "id": list(linhas),
            "valor": list(linhas.values()),
            "_logical_date": ds,
            "_ingested_at": ingerido_em,
        }
    )


def dados(df):
    return dict(zip(df["id"], df["valor"], strict=True))


def changed_at(df):
    return dict(zip(df["id"], df["_changed_at"], strict=True))


class TestRowHash:
    def test_ignora_colunas_de_controle_e_a_ordem_das_colunas(self):
        a = pd.DataFrame({"id": ["1"], "valor": ["x"], "_ingested_at": [T1]})
        b = pd.DataFrame({"valor": ["x"], "_ingested_at": [T2], "id": ["1"], "_logical_date": ["d"]})
        assert merge.row_hash(a).tolist() == merge.row_hash(b).tolist()

    def test_nulos_diferentes_valem_o_mesmo(self):
        a = pd.DataFrame({"id": ["1"], "valor": [None]})
        b = pd.DataFrame({"id": ["1"], "valor": [float("nan")]}).astype({"valor": "object"})
        assert merge.row_hash(a).tolist() == merge.row_hash(b).tolist()

    def test_nulo_e_texto_vazio_nao_se_confundem(self):
        a = pd.DataFrame({"id": ["1"], "valor": [None]})
        b = pd.DataFrame({"id": ["1"], "valor": [""]})
        assert merge.row_hash(a).tolist() != merge.row_hash(b).tolist()

    def test_conteudo_diferente_gera_hash_diferente(self):
        a = pd.DataFrame({"id": ["1"], "valor": ["x"]})
        b = pd.DataFrame({"id": ["1"], "valor": ["y"]})
        assert merge.row_hash(a).tolist() != merge.row_hash(b).tolist()


class TestMergeState:
    def test_primeira_carga_sem_estado_atual(self):
        resultado = merge.merge_state(None, lote("2026-01-01", T1, b="2", a="1"), KEY)
        assert dados(resultado) == {"a": "1", "b": "2"}
        assert resultado["id"].tolist() == ["a", "b"]  # ordenado pela chave
        assert (resultado["_changed_at"] == T1).all()
        assert resultado["_row_hash"].notna().all()

    def test_linha_nova_e_linha_alterada_entram(self):
        atual = merge.merge_state(None, lote("2026-01-01", T1, a="1", b="2"), KEY)
        resultado = merge.merge_state(atual, lote("2026-01-02", T2, b="2-novo", c="3"), KEY)
        assert dados(resultado) == {"a": "1", "b": "2-novo", "c": "3"}

    def test_changed_at_so_avanca_quando_o_conteudo_muda(self):
        atual = merge.merge_state(None, lote("2026-01-01", T1, a="1", b="2"), KEY)
        resultado = merge.merge_state(atual, lote("2026-01-02", T2, a="1", b="2-novo", c="3"), KEY)
        assert changed_at(resultado) == {"a": T1, "b": T2, "c": T2}

    def test_ingested_at_e_logical_date_acompanham_a_versao_vencedora(self):
        atual = merge.merge_state(None, lote("2026-01-01", T1, a="1"), KEY)
        resultado = merge.merge_state(atual, lote("2026-01-02", T2, a="1"), KEY)
        linha = resultado.iloc[0]
        assert (linha["_ingested_at"], linha["_logical_date"]) == (T2, "2026-01-02")
        assert linha["_changed_at"] == T1

    def test_reexecutar_a_mesma_data_e_idempotente(self):
        d1 = lote("2026-01-01", T1, a="1", b="2")
        d2 = lote("2026-01-02", T2, a="1", b="2-novo")
        uma_vez = merge.merge_state(merge.merge_state(None, d1, KEY), d2, KEY)
        duas_vezes = merge.merge_state(uma_vez, d2, KEY)
        pd.testing.assert_frame_equal(uma_vez, duas_vezes)

    def test_data_antiga_nao_sobrescreve_data_nova_mesmo_executada_depois(self):
        """Backfill fora de ordem: a precedência é pela data lógica, não pelo relógio."""
        d1 = lote("2026-01-01", T3, a="1-velho", b="so-no-dia-1")  # ingerida por último (backfill)
        d2 = lote("2026-01-02", T2, a="1-novo")
        estado = merge.merge_state(merge.merge_state(None, d2, KEY), d1, KEY)
        assert dados(estado) == {"a": "1-novo", "b": "so-no-dia-1"}

    def test_ordem_de_execucao_nao_muda_o_conteudo_final(self):
        d1 = lote("2026-01-01", T1, a="1", b="2", c="3")
        d2 = lote("2026-01-02", T2, a="1-novo", d="4")
        d3 = lote("2026-01-03", T3, b="2-novo")
        em_ordem = None
        for d in (d1, d2, d3):
            em_ordem = merge.merge_state(em_ordem, d, KEY)
        embaralhado = None
        for d in (d3, d1, d2):
            embaralhado = merge.merge_state(embaralhado, d, KEY)
        assert dados(em_ordem) == dados(embaralhado)
        assert dados(em_ordem) == {"a": "1-novo", "b": "2-novo", "c": "3", "d": "4"}

    def test_linha_que_some_da_origem_permanece(self):
        atual = merge.merge_state(None, lote("2026-01-01", T1, a="1", b="2"), KEY)
        resultado = merge.merge_state(atual, lote("2026-01-02", T2, a="1"), KEY)
        assert dados(resultado) == {"a": "1", "b": "2"}

    def test_chave_composta(self):
        d1 = pd.DataFrame(
            {"k1": ["a"], "k2": ["x"], "valor": ["1"], "_logical_date": "2026-01-01", "_ingested_at": [T1]}
        )
        d2 = pd.DataFrame(
            {"k1": ["a"], "k2": ["y"], "valor": ["2"], "_logical_date": "2026-01-02", "_ingested_at": [T2]}
        )
        resultado = merge.merge_state(merge.merge_state(None, d1, ["k1", "k2"]), d2, ["k1", "k2"])
        assert len(resultado) == 2

    def test_silver_antiga_sem_colunas_de_controle_e_aceita(self):
        legado = pd.DataFrame({"id": ["a", "b"], "valor": ["1", "2"], "_ingested_at": [T1, T1]})
        resultado = merge.merge_state(legado, lote("2026-01-02", T2, b="2-novo", c="3"), KEY)
        assert dados(resultado) == {"a": "1", "b": "2-novo", "c": "3"}
        assert resultado["_logical_date"].notna().all()
        assert changed_at(resultado)["a"] == T1  # linha antiga sem mudança mantém o _changed_at

    def test_nao_altera_os_dataframes_de_entrada(self):
        atual = merge.merge_state(None, lote("2026-01-01", T1, a="1"), KEY)
        novo = lote("2026-01-02", T2, a="2")
        copia_atual, copia_novo = atual.copy(), novo.copy()
        merge.merge_state(atual, novo, KEY)
        pd.testing.assert_frame_equal(atual, copia_atual)
        pd.testing.assert_frame_equal(novo, copia_novo)


@pytest.mark.parametrize("coluna", merge.CONTROL_COLUMNS)
def test_resultado_tem_as_colunas_de_controle(coluna):
    assert coluna in merge.merge_state(None, lote("2026-01-01", T1, a="1"), KEY).columns
