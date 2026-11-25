import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

sys.path.append("/opt/airflow")

from pipeline.bronze.ingest_clima import main as ingest_clima_main
from pipeline.bronze.ingest_csv import main as ingest_csv_main
from pipeline.common.config import require_env
from pipeline.load.staging import load_to_staging
from pipeline.silver.clean_clientes import main as clean_clientes_silver
from pipeline.silver.clean_clima import main as clean_clima_silver
from pipeline.silver.clean_financeiro import main as clean_financeiro_silver
from pipeline.silver.clean_produtos import main as clean_produtos_silver
from pipeline.silver.clean_vendas import main as clean_vendas_silver

# ---------------------------------------------------------------------------
# Caminho absoluto do projeto dbt no HOST (não dentro do container Airflow).
# O DockerOperator monta este caminho diretamente do host para o container
# dbt que ele vai criar.
# ---------------------------------------------------------------------------
DBT_PROJECT_HOST_PATH = require_env("DBT_PROJECT_HOST_PATH")

# Rede Docker com nome fixo (networks.uberlandia-net.name no docker-compose.yml)
DOCKER_NETWORK = os.getenv("DOCKER_NETWORK", "uberlandia-net")

# Imagem dbt com tag fixa. Fonte única: variável DBT_IMAGE no .env.
DBT_IMAGE = os.getenv("DBT_IMAGE", "ghcr.io/dbt-labs/dbt-postgres:1.8.2")

# Variáveis de ambiente passadas ao container dbt pelo DockerOperator
DBT_ENV_VARS = {
    "POSTGRES_USER": require_env("POSTGRES_USER"),
    "POSTGRES_PASSWORD": require_env("POSTGRES_PASSWORD"),
    "POSTGRES_DB": require_env("POSTGRES_DB"),
    "POSTGRES_HOST": require_env("POSTGRES_HOST"),
    "POSTGRES_PORT": require_env("POSTGRES_PORT"),
    # O container roda com o UID do host (abaixo), que não tem home na imagem:
    # o dbt precisa de um HOME gravável (ex.: dbt deps, ~/.dbt).
    "HOME": "/tmp",
}

# UID:GID do host para o container dbt. Sem isso o dbt roda como root e os arquivos
# que ele grava no bind mount (target/, logs/, dbt_packages/, package-lock.yml)
# ficam com dono root no host.
DBT_CONTAINER_USER = f"{require_env('HOST_UID')}:{require_env('HOST_GID')}"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dag_etl_pipeline",
    default_args=default_args,
    description="Pipeline ETL completo: Bronze → Silver → load_stage → Gold (dbt)",
    schedule_interval="@daily",
    catchup=False,
    # Execuções simultâneas corrompem a Silver: cada uma faz rm + escrita nos mesmos
    # prefixos e os arquivos de ambas se misturam (dados duplicados na Gold).
    # Escrita atômica/idempotente está no backlog (ARQ-01/ARQ-02).
    max_active_runs=1,
    tags=["etl", "pipeline", "dbt", "gold"],
) as dag:
    # -------------------------------------------------------------------------
    # BRONZE — Ingestão de dados brutos
    # -------------------------------------------------------------------------
    ingest_csv_task = PythonOperator(
        task_id="ingest_bronze_csv",
        python_callable=ingest_csv_main,
    )

    ingest_clima_task = PythonOperator(
        task_id="ingest_bronze_clima",
        python_callable=ingest_clima_main,
    )

    # -------------------------------------------------------------------------
    # SILVER — Limpeza e validação de esquema
    # Ordem importa: clean_clientes e clean_produtos DEVEM rodar antes de
    # clean_vendas, pois clean_vendas faz lookup referencial nesses cadastros.
    # -------------------------------------------------------------------------
    clean_clientes_task = PythonOperator(
        task_id="clean_clientes_silver",
        python_callable=clean_clientes_silver,
    )

    clean_produtos_task = PythonOperator(
        task_id="clean_produtos_silver",
        python_callable=clean_produtos_silver,
    )

    clean_financeiro_task = PythonOperator(
        task_id="clean_financeiro_silver",
        python_callable=clean_financeiro_silver,
    )

    clean_clima_task = PythonOperator(
        task_id="clean_clima_silver",
        python_callable=clean_clima_silver,
    )

    # clean_vendas depende de clientes e produtos (lookup referencial)
    clean_vendas_task = PythonOperator(
        task_id="clean_vendas_silver",
        python_callable=clean_vendas_silver,
    )

    # -------------------------------------------------------------------------
    # LOAD — Carga da Silver para o schema load_stage no PostgreSQL
    # -------------------------------------------------------------------------
    task_load_to_staging = PythonOperator(
        task_id="load_to_staging",
        python_callable=load_to_staging,
        execution_timeout=timedelta(minutes=15),
    )

    # -------------------------------------------------------------------------
    # GOLD — Transformações dimensionais via dbt (DockerOperator)
    #
    # Ordem de execução dbt (cada etapa só roda se a anterior tiver sucesso):
    #
    #   0. dbt_deps / dbt_seed: instala pacotes e carrega os seeds de regras de
    #      negócio (regra_tipo_lancamento) e metas (metas_canal_mes) no schema gold.
    #
    #   1. dbt_run_gold: recria as views stg_* (load_stage) e materializa dim_* e
    #      fato_vendas no schema gold. As views stg_* não precisam mais de uma task
    #      própria: load_to_staging faz TRUNCATE + COPY, as tabelas de load_stage
    #      nunca são derrubadas e as views sobrevivem à carga (ARQ-03). Elas entram
    #      na seleção só para acompanhar mudanças nos modelos (create or replace é
    #      barato e idempotente).
    #
    #   2. dbt_test_gold: testes da Gold (integridade, regras, cobertura de clima).
    #
    #   3. dbt_run_analytics: views analíticas vw_q1..vw_q4 consumidas pelo Streamlit.
    #
    # network_mode conecta o container à uberlandia-net para resolução DNS
    # de 'postgres-gold' conforme configurado no profiles.yml.
    # -------------------------------------------------------------------------

    # Argumentos comuns a todos os containers dbt efêmeros
    _dbt_common = dict(
        image=DBT_IMAGE,
        user=DBT_CONTAINER_USER,
        working_dir="/usr/app/dbt",
        environment=DBT_ENV_VARS,
        mounts=[
            Mount(
                source=DBT_PROJECT_HOST_PATH,
                target="/usr/app/dbt",
                type="bind",
                read_only=False,
            ),
        ],
        network_mode=DOCKER_NETWORK,
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
    )

    # Passo 0 — instala pacotes dbt (dbt_packages/ não é mais versionado)
    dbt_deps = DockerOperator(
        task_id="dbt_deps",
        command="deps --profiles-dir /usr/app/dbt",
        execution_timeout=timedelta(minutes=10),
        **_dbt_common,
    )

    # Passo 0.5 — carrega os seeds (regras de negócio e metas) no schema gold
    dbt_seed = DockerOperator(
        task_id="dbt_seed",
        command="seed --profiles-dir /usr/app/dbt",
        execution_timeout=timedelta(minutes=10),
        **_dbt_common,
    )

    # Passo 1 — views stg_* (load_stage) e tabelas/incrementais da camada gold
    dbt_run_gold = DockerOperator(
        task_id="dbt_run_gold",
        command="run --profiles-dir /usr/app/dbt --select staging marts.gold",
        execution_timeout=timedelta(minutes=30),
        **_dbt_common,
    )

    # Passo 2 — testa integridade referencial e accepted_values do schema gold
    dbt_test_gold = DockerOperator(
        task_id="dbt_test_gold",
        command="test --profiles-dir /usr/app/dbt --select marts.gold",
        execution_timeout=timedelta(minutes=15),
        **_dbt_common,
    )

    # Passo 3 — executa todas as views do schema gold
    dbt_run_analytics = DockerOperator(
        task_id="dbt_run_analytics",
        command="run --profiles-dir /usr/app/dbt --select marts.analytics",
        execution_timeout=timedelta(minutes=15),
        **_dbt_common,
    )

    # =========================================================================
    # GRAFO DE DEPENDÊNCIAS
    #
    #  ingest_csv ──► clean_clientes ───┐
    #             ──► clean_produtos ───┴──► clean_vendas ──┐
    #             ──► clean_financeiro ─────────────────────┤
    #  ingest_clima ──► clean_clima ────────────────────────┤
    #                                                       ▼
    #                                              load_to_staging
    #                                                       │
    #   dbt_deps ► dbt_seed ► dbt_run_gold ► dbt_test_gold ► dbt_run_analytics
    # =========================================================================

    # Cadastros independentes disparam em paralelo após CSV Bronze
    ingest_csv_task >> [clean_clientes_task, clean_produtos_task, clean_financeiro_task]

    # Vendas aguarda clientes e produtos (lookup referencial no clean_vendas.py)
    [clean_clientes_task, clean_produtos_task] >> clean_vendas_task

    # Clima segue trilha própria a partir da Bronze
    ingest_clima_task >> clean_clima_task

    # Staging aguarda todos os Silver finalizarem
    [
        clean_vendas_task,
        clean_financeiro_task,
        clean_clima_task,
    ] >> task_load_to_staging

    # dbt: deps → seed → staging + gold → test → analytics
    (task_load_to_staging >> dbt_deps >> dbt_seed >> dbt_run_gold >> dbt_test_gold >> dbt_run_analytics)
