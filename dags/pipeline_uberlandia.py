from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pipeline_uberlandia',
    default_args=default_args,
    description='Pipeline de dados para o case Uberlandia',
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:

    def placeholder_task():
        print("Executando tarefa...")

    task_ingest_bronze = PythonOperator(
        task_id='ingest_bronze',
        python_callable=placeholder_task,
    )
