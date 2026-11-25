"""Checagem da escrita atômica (ARQ-02) contra o MinIO real, com falhas injetadas.

    python scripts/e2e_atomic_check.py

Os testes unitários usam um filesystem em memória; aqui o que se confere é o comportamento do
s3fs/MinIO de verdade ao mover prefixos. Usa um dataset descartável (`silver/_e2e_atomic`) e
termina conferindo que não sobrou `_tmp/` nem `_prev/` em nenhum lugar da Silver.
"""

import sys
from contextlib import suppress
from unittest.mock import patch

import pandas as pd

from pipeline.common.storage import get_s3_client, write_silver

DATASET = "s3://silver/_e2e_atomic/"


def df(*ids):
    return pd.DataFrame({"id": list(ids), "ano": 2024, "mes": 1})


def estado(fs):
    return sorted(pd.read_parquet("silver/_e2e_atomic", filesystem=fs)["id"].tolist())


def main() -> int:
    fs = get_s3_client()
    resultados = []

    def registrar(nome, obtido, esperado):
        ok = obtido == esperado
        print(f"[{'OK' if ok else 'FALHA'}] {nome}: obtido={obtido} esperado={esperado}")
        resultados.append(ok)

    write_silver(fs, df("a", "b"), DATASET, ["ano", "mes"])
    registrar("primeira escrita", estado(fs), ["a", "b"])

    write_silver(fs, df("c"), DATASET, ["ano", "mes"])
    registrar("substituição por uma nova versão", estado(fs), ["c"])

    # Falha ao gravar: a versão atual precisa sobreviver
    with (
        patch.object(pd.DataFrame, "to_parquet", side_effect=OSError("falha injetada na gravação")),
        suppress(OSError),
    ):
        write_silver(fs, df("x"), DATASET, ["ano", "mes"])
    registrar("falha na gravação preserva a versão atual", estado(fs), ["c"])

    # Falha ao publicar (2ª movimentação): a versão antiga precisa ser restaurada
    mv_original = fs.mv
    chamadas = []

    def mv_que_falha_ao_publicar(origem, destino, **kwargs):
        chamadas.append(origem)
        if len(chamadas) == 2:
            raise OSError("falha injetada ao publicar")
        return mv_original(origem, destino, **kwargs)

    with patch.object(fs, "mv", mv_que_falha_ao_publicar), suppress(OSError):
        write_silver(fs, df("y"), DATASET, ["ano", "mes"])
    fs.invalidate_cache()
    registrar("falha ao publicar restaura a versão anterior", estado(fs), ["c"])

    write_silver(fs, df("d", "e"), DATASET, ["ano", "mes"])
    registrar("escrita normal depois das falhas", estado(fs), ["d", "e"])

    fs.rm("silver/_e2e_atomic", recursive=True)
    sobras = [p for p in fs.find("silver") if "/_tmp/" in p or "/_prev/" in p]
    registrar("nenhuma sobra de _tmp/ ou _prev/ na Silver", sobras, [])

    return 0 if all(resultados) else 1


if __name__ == "__main__":
    sys.exit(main())
