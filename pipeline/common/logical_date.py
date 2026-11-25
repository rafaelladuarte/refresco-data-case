"""Data lógica (`ds`) de uma execução: a partição Bronze e o merge da Silver giram em torno dela.

No Airflow o `PythonOperator` injeta `ds` (string `YYYY-MM-DD`) em qualquer callable que tenha um
parâmetro com esse nome. Fora do Airflow (`python -m pipeline.bronze.ingest_csv [YYYY-MM-DD]`),
a ausência de `ds` significa "hoje".
"""

import re
from datetime import date

_PATTERN_DS = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def resolve_ds(ds: str | None = None) -> str:
    """Valida e devolve a data lógica; sem valor, devolve a data de hoje."""
    if ds is None:
        return date.today().isoformat()
    if not _PATTERN_DS.match(ds):
        raise ValueError(f"Data lógica inválida: {ds!r} (esperado YYYY-MM-DD)")
    try:
        date.fromisoformat(ds)
    except ValueError as e:
        raise ValueError(f"Data lógica inválida: {ds!r} ({e})") from e
    return ds
