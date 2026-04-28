from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os

# Adiciona o diretório /opt/airflow ao sys.path para permitir a importação do módulo 'pipeline'
sys.path.append('/opt/airflow')

# Importação das funções de ingestão dos scripts na camada Bronze
from pipeline.bronze.ingest_csv import main as ingest_csv_main
from pipeline.bronze.ingest_clima import main as ingest_clima_main

# Configurações padrão da DAG
default_args = {
    'owner': 'antigravity',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definição da DAG
with DAG(
    'dag_medallion_pipeline',
    default_args=default_args,
    description='Pipeline de dados seguindo a arquitetura Medallion (Ingestão Bronze)',
    schedule_interval='@daily',
    catchup=False,
    tags=['bronze', 'medallion', 'uberlandia'],
) as dag:

    # 1. Task de Verificação: Garante que os diretórios necessários existem
    # Como o volume está mapeado em /opt/airflow/data, verificamos caminhos relativos ou absolutos
    check_folders = BashOperator(
        task_id='check_folders',
        bash_command="""
            if [ -d /opt/airflow/data/raw ] && [ -d /opt/airflow/data/bronze ]; then
                echo "Diretórios de dados encontrados."
            else
                echo "Erro: Diretórios data/raw ou data/bronze não existem."
                exit 1
            fi
        """
    )

    # 2. Task de Ingestão de CSVs (clientes, financeiro, produtos, vendas)
    ingest_csv_task = PythonOperator(
        task_id='ingest_bronze_csv',
        python_callable=ingest_csv_main,
    )

    # 3. Task de Ingestão de Clima (API Open-Meteo)
    ingest_clima_task = PythonOperator(
        task_id='ingest_bronze_clima',
        python_callable=ingest_clima_main,
    )

    # A verificação ocorre primeiro, e as ingestões ocorrem em paralelo
    check_folders >> [ingest_csv_task, ingest_clima_task]
