"""Integridade da DAG. Precisa do Airflow: roda dentro da imagem do Airflow (job e2e).

Sem Airflow instalado (job lint-unit, virtualenv local), o módulo inteiro é pulado.
"""

import os
from pathlib import Path

import pytest

# No e2e (REQUIRE_AIRFLOW=1) a ausência do Airflow é erro, não pulo: um pulo passaria por sucesso.
if os.environ.get("REQUIRE_AIRFLOW") != "1":
    pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402

pytestmark = pytest.mark.airflow

DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"
DAG_ID = "dag_etl_pipeline"

# A DAG exige estas variáveis na importação (falha cedo se faltarem)
ENV_DAG = {
    "DBT_PROJECT_HOST_PATH": "/tmp/dbt",
    "POSTGRES_USER": "u",
    "POSTGRES_PASSWORD": "p",
    "POSTGRES_DB": "gold",
    "POSTGRES_HOST": "postgres-gold",
    "POSTGRES_PORT": "5432",
    "HOST_UID": "1000",
    "HOST_GID": "1000",
}

DEPENDENCIAS_ESPERADAS = {
    "ingest_bronze_csv": set(),
    "ingest_bronze_clima": set(),
    "clean_clientes_silver": {"ingest_bronze_csv"},
    "clean_produtos_silver": {"ingest_bronze_csv"},
    "clean_financeiro_silver": {"ingest_bronze_csv"},
    "clean_clima_silver": {"ingest_bronze_clima"},
    "clean_vendas_silver": {"clean_clientes_silver", "clean_produtos_silver"},
    "load_to_staging": {"clean_vendas_silver", "clean_financeiro_silver", "clean_clima_silver"},
    "dbt_deps": {"load_to_staging"},
    "dbt_seed": {"dbt_deps"},
    "dbt_run_staging": {"dbt_seed"},
    "dbt_run_gold": {"dbt_run_staging"},
    "dbt_test_gold": {"dbt_run_gold"},
    "dbt_run_analytics": {"dbt_test_gold"},
}


@pytest.fixture(scope="module")
def dagbag():
    with pytest.MonkeyPatch.context() as mp:
        for k, v in ENV_DAG.items():
            mp.setenv(k, v)
        # DagBag também verifica ciclos ao carregar
        yield DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_dag_carrega_sem_erros_de_importacao(dagbag):
    assert dagbag.import_errors == {}
    assert DAG_ID in dagbag.dags


def test_quatorze_tasks_com_dependencias_esperadas(dagbag):
    dag = dagbag.dags[DAG_ID]
    assert len(dag.tasks) == 14
    assert {t.task_id: set(t.upstream_task_ids) for t in dag.tasks} == DEPENDENCIAS_ESPERADAS


def test_uma_execucao_ativa_por_vez(dagbag):
    # Execuções simultâneas corrompem a Silver (achado da validação da Sprint 1)
    assert dagbag.dags[DAG_ID].max_active_runs == 1


def test_dbt_roda_com_usuario_do_host(dagbag):
    dag = dagbag.dags[DAG_ID]
    for task in dag.tasks:
        if task.task_id.startswith("dbt_"):
            assert task.user == "1000:1000"
            assert task.environment["HOME"] == "/tmp"


TASKS_POR_DATA_LOGICA = [
    "ingest_bronze_csv",
    "ingest_bronze_clima",
    "clean_clientes_silver",
    "clean_produtos_silver",
    "clean_financeiro_silver",
    "clean_clima_silver",
    "clean_vendas_silver",
]


@pytest.mark.parametrize("task_id", TASKS_POR_DATA_LOGICA)
def test_tasks_bronze_e_silver_recebem_a_data_logica(dagbag, task_id):
    """O PythonOperator injeta `ds` (data lógica) em quem declara o parâmetro: é o que torna a
    partição Bronze e o merge da Silver idempotentes e permite backfill (ARQ-01)."""
    import inspect

    task = dagbag.dags[DAG_ID].get_task(task_id)
    assert "ds" in inspect.signature(task.python_callable).parameters
