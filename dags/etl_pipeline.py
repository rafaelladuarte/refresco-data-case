from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow')

from pipeline.bronze.ingest_csv import main as ingest_csv_main
from pipeline.bronze.ingest_clima import main as ingest_clima_main

from pipeline.silver.clean_vendas import main as clean_vendas_silver
from pipeline.silver.clean_produtos import main as clean_produtos_silver
from pipeline.silver.clean_financeiro import main as clean_financeiro_silver
from pipeline.silver.clean_clientes import main as clean_clientes_silver
from pipeline.silver.clean_clima import main as clean_clima_silver

from pipeline.load.staging import load_to_staging

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
    description='Pipeline ETL com MinIO (Bronze e Silver)',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'pipeline'],
) as dag:

    ingest_csv_task = PythonOperator(
        task_id='ingest_bronze_csv',
        python_callable=ingest_csv_main,
    )

    ingest_clima_task = PythonOperator(
        task_id='ingest_bronze_clima',
        python_callable=ingest_clima_main,
    )

    clean_vendas_task = PythonOperator(
        task_id='clean_vendas_silver',
        python_callable=clean_vendas_silver,
    )

    clean_produtos_task = PythonOperator(
        task_id='clean_produtos_silver',
        python_callable=clean_produtos_silver,
    )

    clean_financeiro_task = PythonOperator(
        task_id='clean_financeiro_silver',
        python_callable=clean_financeiro_silver,
    )

    clean_clientes_task = PythonOperator(
        task_id='clean_clientes_silver',
        python_callable=clean_clientes_silver,
    )

    clean_clima_task = PythonOperator(
        task_id='clean_clima_silver',
        python_callable=clean_clima_silver,
    )

    task_load_to_staging = PythonOperator(
        task_id='load_to_staging',
        python_callable=load_to_staging,
        execution_timeout=timedelta(minutes=15),
    )

    ingest_csv_task >> [
        clean_vendas_task,
        clean_produtos_task,
        clean_financeiro_task,
        clean_clientes_task
    ]

    ingest_clima_task >> clean_clima_task
    
    [
        clean_vendas_task,
        clean_produtos_task,
        clean_financeiro_task,
        clean_clientes_task,
        clean_clima_task
    ] >> task_load_to_staging