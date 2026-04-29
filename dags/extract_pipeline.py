from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow')

from pipeline.bronze.ingest_csv import main as ingest_csv_main
from pipeline.bronze.ingest_clima import main as ingest_clima_main

default_args = {
    'owner': 'antigravity',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag_medallion_pipeline',
    default_args=default_args,
    description='Pipeline Medallion com MinIO',
    schedule_interval='@daily',
    catchup=False,
    tags=['bronze', 'medallion'],
) as dag:

    ingest_csv_task = PythonOperator(
        task_id='ingest_bronze_csv',
        python_callable=ingest_csv_main,
    )

    ingest_clima_task = PythonOperator(
        task_id='ingest_bronze_clima',
        python_callable=ingest_clima_main,
    )

    [ingest_csv_task, ingest_clima_task]