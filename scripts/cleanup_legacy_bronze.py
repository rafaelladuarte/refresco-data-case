"""Remove as partições Bronze antigas (`ingestion_date=<data>T<hora>`, anteriores ao ARQ-01).

    python scripts/cleanup_legacy_bronze.py            # só lista (dry-run)
    python scripts/cleanup_legacy_bronze.py --apply    # apaga

Partições no formato novo (`ingestion_date=YYYY-MM-DD`) nunca são tocadas. A Bronze é só uma cópia
dos CSVs de origem, então apagar as antigas não perde dados: a próxima execução recria o que precisa.
Usa as variáveis de ambiente do pipeline (AWS_*, AIRFLOW__S3__ENDPOINT_URL).
"""

import sys

from pipeline.common.storage import PARTITION_DATE_PATTERN, get_s3_client

TABELAS = ["clientes", "financeiro", "produtos", "vendas", "clima"]


def particoes_antigas(fs) -> list[str]:
    antigas = []
    for tabela in TABELAS:
        for particao in fs.glob(f"bronze/{tabela}/ingestion_date=*"):
            if not PARTITION_DATE_PATTERN.search(particao.rstrip("/")):
                antigas.append(particao)
    return sorted(antigas)


def main(argv: list[str]) -> int:
    aplicar = "--apply" in argv
    fs = get_s3_client()
    antigas = particoes_antigas(fs)

    if not antigas:
        print("Nenhuma partição antiga encontrada.")
        return 0

    for particao in antigas:
        print(f"{'APAGANDO' if aplicar else 'apagaria'}: {particao}")
        if aplicar:
            fs.rm(particao, recursive=True)

    if not aplicar:
        print(f"\n{len(antigas)} partições antigas. Rode com --apply para apagar.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
