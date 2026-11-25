#!/usr/bin/env bash
# Teste ponta a ponta: Bronze -> Silver -> load_stage -> Gold (dbt) -> verificação dos números.
#
# Usado pela CI (job `e2e`) e pode ser rodado localmente:
#
#     python -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
#     source .venv/bin/activate && scripts/e2e.sh
#
# Isolamento: o script NUNCA lê o .env nem usa o ambiente de desenvolvimento. Sem
# E2E_START_SERVICES=0 ele sobe um MinIO e um Postgres descartáveis (portas 19000/15432) e
# os remove ao terminar. As credenciais abaixo são de uso exclusivo do teste.
#
# Variáveis opcionais:
#   E2E_START_SERVICES=0   usa MinIO/Postgres já em execução (CI: service containers)
#   E2E_MINIO_PORT / E2E_PG_PORT   portas dos serviços (padrão 19000 / 15432)
#   E2E_DAG_TEST=0         pula o teste de integridade da DAG (precisa construir a imagem do Airflow)
#   DBT_IMAGE              imagem do dbt (padrão: a mesma da DAG)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

START_SERVICES="${E2E_START_SERVICES:-1}"
MINIO_PORT="${E2E_MINIO_PORT:-19000}"
PG_PORT="${E2E_PG_PORT:-15432}"
DAG_TEST="${E2E_DAG_TEST:-1}"
DBT_IMAGE="${DBT_IMAGE:-ghcr.io/dbt-labs/dbt-postgres:1.8.2}"
MINIO_IMAGE="${E2E_MINIO_IMAGE:-bitnamilegacy/minio:2025.7.23-debian-12-r5}"
PG_IMAGE="${E2E_PG_IMAGE:-postgres:15}"
PYTHON="${PYTHON:-python}"
SUFFIX="$$"

# --- ambiente do teste (sobrescreve qualquer valor herdado do shell) ---
export AWS_ACCESS_KEY_ID="e2e_access_key"
export AWS_SECRET_ACCESS_KEY="e2e_secret_key_123"
export AWS_DEFAULT_REGION="us-east-1"
export AIRFLOW__S3__ENDPOINT_URL="http://localhost:${MINIO_PORT}"
export POSTGRES_HOST="localhost"
export POSTGRES_PORT="${PG_PORT}"
export POSTGRES_DB="gold"
export POSTGRES_USER="e2e_user"
export POSTGRES_PASSWORD="e2e_password"
export AIRFLOW_HOME="$ROOT"                    # ingest_csv lê $AIRFLOW_HOME/data/raw
export CLIMA_FIXTURE_DIR="$ROOT/tests/fixtures/open_meteo"
export CLIMA_ANO_INICIO=2020
export CLIMA_ANO_FIM=2025
export PYTHONPATH="$ROOT"

log() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

CONTAINERS=()
cleanup() {
  local code=$?
  if ((${#CONTAINERS[@]})); then
    log "Removendo serviços do teste"
    docker rm -f "${CONTAINERS[@]}" >/dev/null 2>&1 || true
  fi
  # o dbt grava target/ e logs/ com o UID do host (INF-04); nada a corrigir aqui
  exit "$code"
}
trap cleanup EXIT

wait_for() { # wait_for <descrição> <comando...>
  local desc="$1"; shift
  for _ in $(seq 1 60); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 2
  done
  echo "Timeout aguardando: $desc" >&2
  return 1
}

if [[ "$START_SERVICES" == "1" ]]; then
  log "Subindo MinIO (:${MINIO_PORT}) e Postgres (:${PG_PORT}) descartáveis"
  docker run -d --name "e2e-minio-$SUFFIX" -p "${MINIO_PORT}:9000" \
    -e MINIO_ROOT_USER="$AWS_ACCESS_KEY_ID" -e MINIO_ROOT_PASSWORD="$AWS_SECRET_ACCESS_KEY" \
    "$MINIO_IMAGE" >/dev/null
  CONTAINERS+=("e2e-minio-$SUFFIX")
  docker run -d --name "e2e-pg-$SUFFIX" -p "${PG_PORT}:5432" \
    -e POSTGRES_USER="$POSTGRES_USER" -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" -e POSTGRES_DB="$POSTGRES_DB" \
    "$PG_IMAGE" >/dev/null
  CONTAINERS+=("e2e-pg-$SUFFIX")
fi

log "Aguardando serviços"
wait_for "MinIO" curl -fsS "http://localhost:${MINIO_PORT}/minio/health/live"
wait_for "Postgres" "$PYTHON" -c "
import psycopg2, os
psycopg2.connect(host='localhost', port=os.environ['POSTGRES_PORT'], user=os.environ['POSTGRES_USER'],
                 password=os.environ['POSTGRES_PASSWORD'], dbname=os.environ['POSTGRES_DB']).close()"

log "Bronze"
"$PYTHON" -m pipeline.bronze.ingest_csv
"$PYTHON" -m pipeline.bronze.ingest_clima

log "Silver"
"$PYTHON" -m pipeline.silver.clean_clientes
"$PYTHON" -m pipeline.silver.clean_produtos
"$PYTHON" -m pipeline.silver.clean_financeiro
"$PYTHON" -m pipeline.silver.clean_clima
"$PYTHON" -m pipeline.silver.clean_vendas

log "Carga no load_stage"
"$PYTHON" -m pipeline.load.staging

dbt() {
  # --network host: o profiles.yml lê POSTGRES_HOST/PORT; o container enxerga o Postgres em localhost.
  docker run --rm --network host --user "$(id -u):$(id -g)" \
    -e HOME=/tmp -e POSTGRES_HOST -e POSTGRES_PORT -e POSTGRES_DB -e POSTGRES_USER -e POSTGRES_PASSWORD \
    -v "$ROOT/dbt:/usr/app/dbt" -w /usr/app/dbt \
    "$DBT_IMAGE" "$@" --profiles-dir /usr/app/dbt
}

log "dbt deps / seed / build"
dbt deps
dbt seed
dbt build

log "Verificação dos números de referência"
"$PYTHON" scripts/e2e_verify.py

if [[ "$DAG_TEST" == "1" ]]; then
  log "Integridade da DAG (imagem do Airflow)"
  docker build -q -t refresco-airflow-e2e .
  # Usuário padrão da imagem (airflow): é nele que o Airflow está instalado. Repo somente leitura;
  # REQUIRE_AIRFLOW=1 faz o teste falhar (em vez de ser pulado) se o Airflow não for importável.
  docker run --rm -e REQUIRE_AIRFLOW=1 -e PYTHONDONTWRITEBYTECODE=1 -e PYTHONPATH=/tmp/pt \
    -v "$ROOT:/opt/airflow/repo:ro" -w /opt/airflow/repo --entrypoint bash refresco-airflow-e2e -c \
    "pip install -q --no-warn-script-location --target /tmp/pt 'pytest==8.3.4' && \
     python -m pytest tests/integration -m airflow -p no:cacheprovider"
fi

log "E2E concluído com sucesso"
