FROM apache/airflow:2.9.0-python3.12

# Versões usadas para resolver o constraints file oficial do Airflow.
# Devem acompanhar a tag da imagem base acima.
ARG AIRFLOW_VERSION=2.9.0
ARG PYTHON_VERSION=3.12

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt

# apache-airflow é fixado explicitamente na mesma versão da imagem para que o pip
# nunca o atualize/rebaixe ao resolver as dependências do projeto. O constraints
# file oficial garante versões compatíveis e testadas com esta versão do Airflow.
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
