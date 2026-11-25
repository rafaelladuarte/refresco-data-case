"""scripts/fingerprint.py: o hash precisa ignorar ordem e colunas de controle, e só isso."""

import fingerprint as fp
import pandas as pd


def df(**colunas):
    return pd.DataFrame(colunas)


class TestFingerprint:
    def test_ignora_a_ordem_das_linhas(self):
        a = df(id=["1", "2", "3"], valor=[10, 20, 30])
        b = df(id=["3", "1", "2"], valor=[30, 10, 20])
        assert fp.fingerprint(a) == fp.fingerprint(b)

    def test_ignora_a_ordem_das_colunas(self):
        a = df(id=["1", "2"], valor=[10, 20])
        assert fp.fingerprint(a) == fp.fingerprint(a[["valor", "id"]])

    def test_ignora_colunas_de_controle(self):
        base = df(id=["1", "2"], valor=[10, 20])
        com_controle = base.assign(
            _ingested_at=["2026-01-01", "2026-01-02"], _changed_at="x", _logical_date="y", _row_hash="z"
        )
        assert fp.fingerprint(base) == fp.fingerprint(com_controle)

    def test_detecta_mudanca_de_valor(self):
        a = df(id=["1", "2"], valor=[10, 20])
        b = df(id=["1", "2"], valor=[10, 21])
        assert fp.fingerprint(a)["sha256"] != fp.fingerprint(b)["sha256"]

    def test_detecta_linha_a_mais_e_coluna_de_negocio_a_mais(self):
        a = df(id=["1", "2"], valor=[10, 20])
        assert fp.fingerprint(a) != fp.fingerprint(df(id=["1", "2", "3"], valor=[10, 20, 30]))
        assert fp.fingerprint(a) != fp.fingerprint(a.assign(extra=1))

    def test_nulo_e_texto_vazio_nao_se_confundem_com_valor(self):
        a = df(id=["1"], valor=[None])
        b = df(id=["1"], valor=["0"])
        assert fp.fingerprint(a)["sha256"] != fp.fingerprint(b)["sha256"]

    def test_informa_linhas_e_colunas_ordenadas(self):
        resultado = fp.fingerprint(df(b=[1, 2], a=[3, 4]))
        assert resultado["linhas"] == 2
        assert resultado["colunas"] == ["a", "b"]


class TestComparar:
    A = {"gold.fato": {"linhas": 2, "sha256": "x"}, "silver/vendas": {"linhas": 3, "sha256": "y"}}

    def test_iguais_retorna_zero(self, capsys):
        assert fp.comparar(self.A, dict(self.A)) == 0
        assert "2 datasets idênticos, 0 diferentes" in capsys.readouterr().out

    def test_conteudo_diferente_retorna_um_e_nomeia_o_dataset(self, capsys):
        b = {**self.A, "gold.fato": {"linhas": 2, "sha256": "outro"}}
        assert fp.comparar(self.A, b) == 1
        assert "[DIFERENTE] gold.fato" in capsys.readouterr().out

    def test_dataset_que_aparece_ou_some_e_diferenca(self, capsys):
        sem_vendas = {"gold.fato": self.A["gold.fato"]}
        com_novo = {**self.A, "gold.nova": {"linhas": 1, "sha256": "z"}}
        assert fp.comparar(self.A, sem_vendas) == 1
        assert fp.comparar(self.A, com_novo) == 1
        saida = capsys.readouterr().out
        assert "só no primeiro" in saida
        assert "só no segundo" in saida
