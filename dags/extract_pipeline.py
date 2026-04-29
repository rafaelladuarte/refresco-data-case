from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow')

# Importações da Camada Bronze
from pipeline.bronze.ingest_csv import main as ingest_csv_main
from pipeline.bronze.ingest_clima import main as ingest_clima_main

# Importações da Camada Silver
from pipeline.silver.clean_vendas import main as clean_vendas_silver
from pipeline.silver.clean_produtos import main as clean_produtos_silver
from pipeline.silver.clean_financeiro import main as clean_financeiro_silver
from pipeline.silver.clean_clientes import main as clean_clientes_silver
from pipeline.silver.clean_clima import main as clean_clima_silver

default_args = {
    'owner': 'antigravity',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag_medallion_etl_pipeline',
    default_args=default_args,
    description='Pipeline Medallion com MinIO (Bronze e Silver)',
    schedule_interval='@daily',
    catchup=False,
    tags=['bronze', 'silver', 'medallion'],
) as dag:

    # ==========================
    # TASKS DA CAMADA BRONZE
    # ==========================
    ingest_csv_task = PythonOperator(
        task_id='ingest_bronze_csv',
        python_callable=ingest_csv_main,
    )

    ingest_clima_task = PythonOperator(
        task_id='ingest_bronze_clima',
        python_callable=ingest_clima_main,
    )

    # ==========================
    # TASKS DA CAMADA SILVER
    # ==========================
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

    # ==========================
    # DEFINIÇÃO DE DEPENDÊNCIAS
    # ==========================
    
    # Paralelismo inicial: CSV e Clima começam juntos (já garantido por não terem upstreams)
    
    # Ramificação da ingestão de CSVs locais para os respectivos limpadores da Silver
    ingest_csv_task >> [
        clean_vendas_task,
        clean_produtos_task,
        clean_financeiro_task,
        clean_clientes_task
    ]

    # Fluxo isolado do Clima
    ingest_clima_task >> clean_clima_task