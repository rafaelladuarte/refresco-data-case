"""Auditoria de qualidade: cada task Silver grava em `audit.dq_results` o que leu, aprovou e rejeitou.

A tabela é definida em `pipeline/ddl/audit.sql`. Reexecutar o mesmo `run_id` de uma entidade
substitui as linhas dela (idempotente).
"""

import re
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

from pipeline.common.config import postgres_url
from pipeline.common.logging import get_logger

logger = get_logger(__name__)

DDL_PATH = Path(__file__).resolve().parents[1] / "ddl" / "audit.sql"
# Várias tasks Silver rodam em paralelo e aplicam a DDL: o lock evita a corrida do CREATE ... IF NOT EXISTS
_DDL_LOCK_ID = 7_301_001


def resolve_run_id(run_id: str | None, ds: str) -> str:
    """O Airflow injeta `run_id`. Fora dele, `manual__<ds>`: reexecutar a mesma data substitui a auditoria."""
    return run_id or f"manual__{ds}"


def motivos_agregados(motivos: pd.Series) -> dict[str, int]:
    """Conta registros por regra violada, sem o valor específico.

    O texto do motivo pode combinar várias regras (separadas por ' | ') e trazer o valor ofensor
    ("id_cliente inválido ou vazio: 'X'", "CNPJ duplicado de C0001"). A regra é o que vem antes do
    ':' e sem o " de <ID>" final, para que registros diferentes da mesma regra somem juntos.
    """
    contagem: dict[str, int] = {}
    for motivo in motivos.dropna():
        regras = {_regra(parte) for parte in str(motivo).split(" | ")}
        for regra in regras:
            contagem[regra] = contagem.get(regra, 0) + 1
    return dict(sorted(contagem.items()))


def _regra(parte: str) -> str:
    regra = parte.split(":")[0].strip()
    return re.sub(r"\s+de\s+[A-Z]\d+$", "", regra)


def build_rows(
    entidade: str,
    ds: str,
    run_id: str,
    lidas: int,
    aprovadas: int,
    quarentena: pd.DataFrame,
) -> list[dict]:
    """Linhas de auditoria de uma entidade: o resumo e uma por motivo de rejeição.

    `duplicadas` é o que sobra entre lidas, aprovadas e rejeitadas. Se der negativo, um registro
    foi contado como aprovado e rejeitado ao mesmo tempo, o que é um bug: falha em vez de gravar.
    """
    rejeitadas = len(quarentena)
    duplicadas = lidas - aprovadas - rejeitadas
    if duplicadas < 0:
        raise ValueError(
            f"Auditoria de {entidade}: lidas={lidas} é menor que "
            f"aprovadas={aprovadas} + rejeitadas={rejeitadas}"
        )

    base = {"run_id": run_id, "entidade": entidade, "data_logica": ds}
    linhas = [
        {
            **base,
            "lidas": lidas,
            "aprovadas": aprovadas,
            "duplicadas": duplicadas,
            "rejeitadas": rejeitadas,
            "motivo": None,
        }
    ]
    if "motivo_rejeicao" in quarentena:
        for motivo, quantidade in motivos_agregados(quarentena["motivo_rejeicao"]).items():
            linhas.append(
                {
                    **base,
                    "lidas": None,
                    "aprovadas": None,
                    "duplicadas": None,
                    "rejeitadas": quantidade,
                    "motivo": motivo,
                }
            )
    return linhas


def ensure_audit_table(conn) -> None:
    """Aplica `pipeline/ddl/audit.sql` (idempotente, protegido contra execução concorrente)."""
    conn.execute(text("SELECT pg_advisory_xact_lock(:id)"), {"id": _DDL_LOCK_ID})
    conn.exec_driver_sql(DDL_PATH.read_text(encoding="utf-8"))


def write_audit(engine, linhas: list[dict]) -> None:
    """Substitui, numa transação, as linhas de auditoria do (run_id, entidade) pelas informadas."""
    run_id, entidade = linhas[0]["run_id"], linhas[0]["entidade"]
    with engine.begin() as conn:
        ensure_audit_table(conn)
        conn.execute(
            text("DELETE FROM audit.dq_results WHERE run_id = :run_id AND entidade = :entidade"),
            {"run_id": run_id, "entidade": entidade},
        )
        conn.execute(
            text(
                "INSERT INTO audit.dq_results "
                "(run_id, entidade, data_logica, lidas, aprovadas, duplicadas, rejeitadas, motivo) "
                "VALUES (:run_id, :entidade, :data_logica, :lidas, :aprovadas, :duplicadas, "
                ":rejeitadas, :motivo)"
            ),
            linhas,
        )


def record_dq(
    entidade: str,
    ds: str,
    run_id: str | None,
    lidas: int,
    aprovadas: int,
    quarentena: pd.DataFrame,
) -> None:
    """Grava a auditoria de uma entidade no Postgres da Gold."""
    run_id = resolve_run_id(run_id, ds)
    linhas = build_rows(entidade, ds, run_id, lidas, aprovadas, quarentena)
    write_audit(create_engine(postgres_url()), linhas)
    resumo = linhas[0]
    logger.info(
        f"[AUDITORIA] {entidade} ({run_id}): lidas={resumo['lidas']} aprovadas={resumo['aprovadas']} "
        f"duplicadas={resumo['duplicadas']} rejeitadas={resumo['rejeitadas']}"
    )
