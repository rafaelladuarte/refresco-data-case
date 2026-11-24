from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta
import sys
import os

sys.path.append('/opt/airflow')

from pipeline.bronze.ingest_csv import main as ingest_csv_main
from pipeline.bronze.ingest_clima import main as ingest_clima_main

from pipeline.silver.clean_vendas import main as clean_vendas_silver
from pipeline.silver.clean_produtos import main as clean_produtos_silver
from pipeline.silver.clean_financeiro import main as clean_financeiro_silver
from pipeline.silver.clean_clientes import main as clean_clientes_silver
from pipeline.silver.clean_clima import main as clean_clima_silver

from pipeline.load.staging import load_to_staging

# ---------------------------------------------------------------------------
# Caminho absoluto do projeto dbt no HOST (não dentro do container Airflow).
# O DockerOperator monta este caminho diretamente do host para o container
# dbt que ele vai criar.
# ---------------------------------------------------------------------------
def _require_env(name):
    """Falha explicitamente se a variável de ambiente obrigatória não existir."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Variável de ambiente obrigatória não definida: {name}")
    return value


DBT_PROJECT_HOST_PATH = _require_env('DBT_PROJECT_HOST_PATH')

# Rede Docker com nome fixo (networks.uberlandia-net.name no docker-compose.yml)
DOCKER_NETWORK = os.getenv('DOCKER_NETWORK', 'uberlandia-net')

# Imagem dbt com tag fixa. Fonte única: variável DBT_IMAGE no .env.
DBT_IMAGE = os.getenv('DBT_IMAGE', 'ghcr.io/dbt-labs/dbt-postgres:1.8.2')

# Variáveis de ambiente passadas ao container dbt pelo DockerOperator
DBT_ENV_VARS = {
    'POSTGRES_USER': _require_env('POSTGRES_USER'),
    'POSTGRES_PASSWORD': _require_env('POSTGRES_PASSWORD'),
    'POSTGRES_DB': _require_env('POSTGRES_DB'),
    'POSTGRES_HOST': _require_env('POSTGRES_HOST'),
    'POSTGRES_PORT': _require_env('POSTGRES_PORT'),
}

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag_etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL completo: Bronze → Silver → load_stage → Gold (dbt)',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'pipeline', 'dbt', 'gold'],
) as dag:

    # -------------------------------------------------------------------------
    # BRONZE — Ingestão de dados brutos
    # -------------------------------------------------------------------------
    ingest_csv_task = PythonOperator(
        task_id='ingest_bronze_csv',
        python_callable=ingest_csv_main,
    )

    ingest_clima_task = PythonOperator(
        task_id='ingest_bronze_clima',
        python_callable=ingest_clima_main,
    )

    # -------------------------------------------------------------------------
    # SILVER — Limpeza e validação de esquema
    # Ordem importa: clean_clientes e clean_produtos DEVEM rodar antes de
    # clean_vendas, pois clean_vendas faz lookup referencial nesses cadastros.
    # -------------------------------------------------------------------------
    clean_clientes_task = PythonOperator(
        task_id='clean_clientes_silver',
        python_callable=clean_clientes_silver,
    )

    clean_produtos_task = PythonOperator(
        task_id='clean_produtos_silver',
        python_callable=clean_produtos_silver,
    )

    clean_financeiro_task = PythonOperator(
        task_id='clean_financeiro_silver',
        python_callable=clean_financeiro_silver,
    )

    clean_clima_task = PythonOperator(
        task_id='clean_clima_silver',
        python_callable=clean_clima_silver,
    )

    # clean_vendas depende de clientes e produtos (lookup referencial)
    clean_vendas_task = PythonOperator(
        task_id='clean_vendas_silver',
        python_callable=clean_vendas_silver,
    )

    # -------------------------------------------------------------------------
    # LOAD — Carga da Silver para o schema load_stage no PostgreSQL
    # -------------------------------------------------------------------------
    task_load_to_staging = PythonOperator(
        task_id='load_to_staging',
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
    #   1. dbt_run_staging: recria as views stg_* no schema load_stage.
    #      Necessário a cada execução porque load_to_staging faz
    #      DROP TABLE ... CASCADE nas tabelas de load_stage, o que remove as views
    #      dependentes. (Não tem relação com o container dbt ser efêmero: as views
    #      ficam no PostgreSQL.) Trocar por TRUNCATE + COPY está no card ARQ-03.
    #
    #   2. dbt_run_gold: materializa dim_* e fato_vendas no schema gold.
    #
    #   3. dbt_test_gold: testes da Gold (integridade, regras, cobertura de clima).
    #
    #   4. dbt_run_analytics: views analíticas vw_q1..vw_q4 consumidas pelo Streamlit.
    #
    # network_mode conecta o container à uberlandia-net para resolução DNS
    # de 'postgres-gold' conforme configurado no profiles.yml.
    # -------------------------------------------------------------------------

    # Argumentos comuns a todos os containers dbt efêmeros
    _dbt_common = dict(
        image=DBT_IMAGE,
        working_dir='/usr/app/dbt',
        environment=DBT_ENV_VARS,
        mounts=[
            Mount(
                source=DBT_PROJECT_HOST_PATH,
                target='/usr/app/dbt',
                type='bind',
                read_only=False,
            ),
        ],
        network_mode=DOCKER_NETWORK,
        auto_remove='success',
        docker_url='unix://var/run/docker.sock',
        mount_tmp_dir=False,
    )

    # Passo 0 — instala pacotes dbt (dbt_packages/ não é mais versionado)
    dbt_deps = DockerOperator(
        task_id='dbt_deps',
        command='deps --profiles-dir /usr/app/dbt',
        execution_timeout=timedelta(minutes=10),
        **_dbt_common,
    )

    # Passo 0.5 — carrega os seeds (regras de negócio e metas) no schema gold
    dbt_seed = DockerOperator(
        task_id='dbt_seed',
        command='seed --profiles-dir /usr/app/dbt',
        execution_timeout=timedelta(minutes=10),
        **_dbt_common,
    )

    # Passo 1 — recria as views stg_* no schema load_stage
    dbt_run_staging = DockerOperator(
        task_id='dbt_run_staging',
        command='run --profiles-dir /usr/app/dbt --select staging',
        execution_timeout=timedelta(minutes=15),
        **_dbt_common,
    )

    # Passo 2 — materializa as tabelas/incrementais da camada gold
    dbt_run_gold = DockerOperator(
        task_id='dbt_run_gold',
        command='run --profiles-dir /usr/app/dbt --select marts.gold',
        execution_timeout=timedelta(minutes=30),
        **_dbt_common,
    )

    # Passo 3 — testa integridade referencial e accepted_values do schema gold
    dbt_test_gold = DockerOperator(
        task_id='dbt_test_gold',
        command='test --profiles-dir /usr/app/dbt --select marts.gold',
        execution_timeout=timedelta(minutes=15),
        **_dbt_common,
    )

    # Passo 4 — executa todas as views do schema gold
    dbt_run_analytics = DockerOperator(
        task_id='dbt_run_analytics',
        command='run --profiles-dir /usr/app/dbt --select marts.analytics',
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
    #   dbt_deps ► dbt_seed ► dbt_run_staging ► dbt_run_gold ► dbt_test_gold ► dbt_run_analytics
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

    # dbt: deps → seed → staging → gold → test → analytics
    task_load_to_staging >> dbt_deps >> dbt_seed >> dbt_run_staging >> dbt_run_gold >> dbt_test_gold >> dbt_run_analytics