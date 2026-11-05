#!/usr/bin/env bash
# Dispara ecommerce_pipeline y espera el resultado.
#   ./scripts/run_pipeline.sh [--batch-id ID] [--mode incremental|full] [--fail-task TASK_ID]
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
require_docker
require_env_file

batch_id=""
mode="incremental"
fail_task=""
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --batch-id) batch_id="$2"; shift 2 ;;
    --mode) mode="$2"; shift 2 ;;
    --fail-task) fail_task="$2"; shift 2 ;;
    *) printf 'Usage: %s [--batch-id ID] [--mode incremental|full] [--fail-task TASK_ID]\n' "$0" >&2; exit 2 ;;
  esac
done

dag_available=false
for _ in {1..60}; do
  if compose exec -T airflow-scheduler python -c 'from airflow.models.dag import DagModel; from airflow.settings import Session; session = Session(); ready = session.query(DagModel).filter(DagModel.dag_id == "ecommerce_pipeline").count() == 1; session.close(); raise SystemExit(0 if ready else 1)' >/dev/null 2>&1; then
    dag_available=true
    break
  fi
  sleep 2
done
[[ "$dag_available" == true ]] || {
  printf 'Error: ecommerce_pipeline is not registered. Check: docker compose logs airflow-scheduler\n' >&2
  exit 1
}
compose exec -T airflow-scheduler airflow dags unpause ecommerce_pipeline >/dev/null

execution_date="$(date -u +'%Y-%m-%dT%H:%M:%S+00:00')"
[[ -n "$batch_id" ]] || batch_id="$(date -u -d "$execution_date" +'%Y%m%dT%H%M%SZ')"
conf="$(jq -cn --arg b "$batch_id" --arg m "$mode" --arg f "$fail_task" '{batch_id: $b, mode: $m, fail_task: $f}')"
printf 'Triggering ecommerce_pipeline at %s with conf %s\n' "$execution_date" "$conf"
compose exec -T airflow-scheduler airflow dags trigger --exec-date "$execution_date" --conf "$conf" ecommerce_pipeline >/dev/null

for _ in {1..240}; do
  # El estado se lee de la base de metadatos por run_id: la CLI mezcla avisos con el resultado.
  state="$(compose exec -T airflow-scheduler python -c "
from airflow.models.dagrun import DagRun
from airflow.settings import Session
session = Session()
run = session.query(DagRun).filter(DagRun.dag_id == 'ecommerce_pipeline', DagRun.run_id == 'manual__${execution_date}').first()
print(run.state if run else '')
session.close()" 2>/dev/null | tr -d '\r' | grep -E '^(success|failed|running|queued)$' | tail -n 1 || true)"
  case "$state" in
    success)
      printf 'ecommerce_pipeline completed successfully. batch_id=%s\n' "$batch_id"
      exit 0
      ;;
    failed)
      printf 'Error: ecommerce_pipeline failed. batch_id=%s. Check Airflow task logs and ops.batch_control.\n' "$batch_id" >&2
      exit 1
      ;;
  esac
  sleep 5
done
printf 'Error: ecommerce_pipeline did not finish within 20 minutes. batch_id=%s\n' "$batch_id" >&2
exit 1
