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
#   E2E_DAY2=0             pula o cenário do dia 2 (dados alterados na origem, segunda execução)
#   E2E_ENFORCE_PENDING=1  no dia 2, checagens ainda pendentes de um card passam a falhar
#   E2E_UPDATE_BASELINE=1  regrava tests/baseline/fingerprint.json com o resultado do dia 1. Use só
#                          quando uma mudança de dados for intencional e justificada no commit.
#   DBT_IMAGE              imagem do dbt (padrão: a mesma da DAG)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

START_SERVICES="${E2E_START_SERVICES:-1}"
MINIO_PORT="${E2E_MINIO_PORT:-19000}"
PG_PORT="${E2E_PG_PORT:-15432}"
DAG_TEST="${E2E_DAG_TEST:-1}"
DAY2="${E2E_DAY2:-1}"
BASELINE="$ROOT/tests/baseline/fingerprint.json"
WORKDIR="$(mktemp -d)"
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
  rm -rf "$WORKDIR"
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

# Datas lógicas do cenário (a DAG passa `ds` do Airflow; aqui vão como argumento)
DS_DIA1="2024-06-01"
DS_DIA2="2024-06-02"

# run_pipeline <ds>: Bronze -> Silver -> load_stage para a data lógica informada
run_pipeline() {
  local ds="$1"
  log "Bronze ($ds)"
  "$PYTHON" -m pipeline.bronze.ingest_csv "$ds"
  "$PYTHON" -m pipeline.bronze.ingest_clima "$ds"

  log "Silver ($ds)"
  "$PYTHON" -m pipeline.silver.clean_clientes "$ds" "e2e-$ds"
  "$PYTHON" -m pipeline.silver.clean_produtos "$ds" "e2e-$ds"
  "$PYTHON" -m pipeline.silver.clean_financeiro "$ds" "e2e-$ds"
  "$PYTHON" -m pipeline.silver.clean_clima "$ds" "e2e-$ds"
  "$PYTHON" -m pipeline.silver.clean_vendas "$ds" "e2e-$ds"

  log "Carga no load_stage"
  "$PYTHON" -m pipeline.load.staging
}

dbt() {
  # --network host: o profiles.yml lê POSTGRES_HOST/PORT; o container enxerga o Postgres em localhost.
  docker run --rm --network host --user "$(id -u):$(id -g)" \
    -e HOME=/tmp -e POSTGRES_HOST -e POSTGRES_PORT -e POSTGRES_DB -e POSTGRES_USER -e POSTGRES_PASSWORD \
    -v "$ROOT/dbt:/usr/app/dbt" -w /usr/app/dbt \
    "$DBT_IMAGE" "$@" --profiles-dir /usr/app/dbt
}

# ---------------------------------------------------------------- dia 1
run_pipeline "$DS_DIA1"

log "dbt deps / seed / build"
dbt deps
dbt seed
dbt build

log "Verificação dos números de referência"
E2E_DS="$DS_DIA1" "$PYTHON" scripts/e2e_verify.py

log "Escrita atômica contra o MinIO real (falhas injetadas)"
"$PYTHON" scripts/e2e_atomic_check.py

# Fingerprint: o conteúdo de cada dataset do dia 1 precisa ser idêntico ao baseline versionado.
# Só muda por decisão explícita (E2E_UPDATE_BASELINE=1), justificada no commit.
log "Fingerprint dos datasets (dia 1)"
"$PYTHON" scripts/fingerprint.py gerar "$WORKDIR/fingerprint.json"
if [[ "${E2E_UPDATE_BASELINE:-0}" == "1" || ! -f "$BASELINE" ]]; then
  mkdir -p "$(dirname "$BASELINE")"
  cp "$WORKDIR/fingerprint.json" "$BASELINE"
  echo "Baseline gravado em $BASELINE"
else
  "$PYTHON" scripts/fingerprint.py comparar "$BASELINE" "$WORKDIR/fingerprint.json"
fi

# ---------------------------------------------------------------- dia 2
# Segunda execução sobre uma cópia de data/raw com três mudanças conhecidas (status financeiro de
# uma venda antiga, uma venda retroativa e o nome de um cliente). Só elas podem aparecer.
if [[ "$DAY2" == "1" ]]; then
  log "Dia 2: dados alterados na origem"
  "$PYTHON" scripts/e2e_mutate.py "$ROOT/data/raw" "$WORKDIR/data/raw" "$WORKDIR/mudancas.json"
  AIRFLOW_HOME="$WORKDIR" run_pipeline "$DS_DIA2"

  # ARQ-03: o load_to_staging fez TRUNCATE + COPY, então as views stg_* do dia 1 continuam de pé e
  # já enxergam os dados do dia 2, antes de qualquer dbt run.
  log "Views stg_* depois da carga (sem dbt)"
  "$PYTHON" scripts/e2e_views_check.py "$WORKDIR/mudancas.json"

  # MOD-01 pendente: o incremental perde a venda retroativa e o teste de reconciliação (MOD-02)
  # acusa. Tolera só essa falha, e só enquanto E2E_ENFORCE_PENDING != 1; qualquer outra reprova.
  log "dbt build (dia 2)"
  dbt build || true
  "$PYTHON" - <<'PY'
import json, os, sys

TOLERADAS = {"assert_fato_reconcilia_com_staging"}  # remover quando o MOD-01 for implementado
resultados = json.load(open("dbt/target/run_results.json"))["results"]
falhas = sorted(r["unique_id"].split(".")[-1] for r in resultados if r["status"] in ("fail", "error"))
if os.environ.get("E2E_ENFORCE_PENDING") == "1":
    inesperadas = falhas
else:
    inesperadas = [f for f in falhas if f not in TOLERADAS]
for f in falhas:
    print(f"[{'FALHA' if f in inesperadas else 'PENDENTE'}] dbt (dia 2): {f}")
sys.exit(1 if inesperadas else 0)
PY
  E2E_DS="$DS_DIA2" "$PYTHON" scripts/e2e_verify_day2.py "$WORKDIR/mudancas.json"

  # Idempotência e backfill fora de ordem (ARQ-01). O estado atual (dia 2) é a referência.
  log "Fingerprint do estado após o dia 2"
  "$PYTHON" scripts/fingerprint.py gerar "$WORKDIR/fingerprint_dia2.json"

  # Reexecuta o dia 2: mesma data lógica, mesmos dados -> nada pode mudar
  log "Reexecução do dia 2 (idempotência)"
  AIRFLOW_HOME="$WORKDIR" run_pipeline "$DS_DIA2"
  "$PYTHON" scripts/fingerprint.py gerar "$WORKDIR/fingerprint_reexec.json"
  "$PYTHON" scripts/fingerprint.py comparar "$WORKDIR/fingerprint_dia2.json" "$WORKDIR/fingerprint_reexec.json"

  # Reexecuta o dia 1 DEPOIS do dia 2 (backfill fora de ordem): a data lógica mais antiga não pode
  # desfazer as mudanças do dia 2 na Silver. Compara só a Silver: o dbt não rodou de novo.
  log "Backfill fora de ordem: dia 1 depois do dia 2"
  run_pipeline "$DS_DIA1"
  "$PYTHON" scripts/fingerprint.py gerar "$WORKDIR/fingerprint_backfill.json"
  "$PYTHON" scripts/fingerprint.py comparar "$WORKDIR/fingerprint_dia2.json" "$WORKDIR/fingerprint_backfill.json"
fi

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
