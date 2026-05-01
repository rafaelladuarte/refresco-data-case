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
DBT_PROJECT_HOST_PATH = os.getenv(
    'DBT_PROJECT_HOST_PATH',
    '/home/elladarte/Documentos/teste-tecnico-uberlandia-refresco/refresco-data-case/dbt'
)

# Nome da rede Docker Compose (prefixo do projeto + nome no YAML)
DOCKER_NETWORK = os.getenv('DOCKER_NETWORK', 'refresco-data-case_uberlandia-net')

# Imagem dbt (mesma do docker-compose.yml)
DBT_IMAGE = 'ghcr.io/dbt-labs/dbt-postgres:1.8.latest'

# Variáveis de ambiente passadas ao container dbt pelo DockerOperator
DBT_ENV_VARS = {
    'POSTGRES_USER': os.getenv('POSTGRES_USER', 'uberlandia'),
    'POSTGRES_PASSWORD': os.getenv('POSTGRES_PASSWORD', 'senha123'),
    'POSTGRES_DB': os.getenv('POSTGRES_DB', 'gold'),
    'POSTGRES_HOST': os.getenv('POSTGRES_HOST', 'postgres-gold'),
    'POSTGRES_PORT': os.getenv('POSTGRES_PORT', '5432'),
}

default_args = {
    'owner': 'antigravity',
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
    # ATENÇÃO — ordem obrigatória de execução dbt:
    #
    #   1. dbt_run_staging: cria (ou recria) as views stg_* no schema load_stage.
    #      Cada DockerOperator cria um container efêmero sem estado. As views de
    #      staging não persistem entre execuções de DAG — precisam ser recriadas
    #      antes que os modelos gold possam referenciá-las via ref().
    #
    #   2. dbt_run_gold: materializa dim_* e fato_vendas no schema gold.
    #      Referencia as views de staging via ref('stg_xxx'), que agora existem.
    #
    #   3. dbt_test_gold: valida integridade referencial e accepted_values.
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

    # =========================================================================
    # GRAFO DE DEPENDÊNCIAS
    #
    #  ingest_csv_task ──► clean_clientes_task ──┐
    #                  ──► clean_produtos_task  ──┼──► clean_vendas_task ──┐
    #                  ──► clean_financeiro_task──┘                        │
    #                                                                       ▼
    #  ingest_clima_task ──► clean_clima_task ──────────────────► task_load_to_staging
    #                                                                       │
    #                                                              dbt_run_gold
    #                                                                       │
    #                                                             dbt_test_gold
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

    # dbt: staging → gold → test (cada etapa só roda se a anterior tiver sucesso)
    task_load_to_staging >> dbt_run_staging >> dbt_run_gold >> dbt_test_gold