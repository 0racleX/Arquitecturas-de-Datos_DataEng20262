#!/usr/bin/env bash
# [EC2, root] Lo ejecuta user_data en el primer arranque (y se puede repetir sin riesgo):
# genera .env, levanta Airflow en Docker, carga las fuentes iniciales y activa el DAG.
set -euo pipefail
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

find "$PROJECT_ROOT/scripts" -name '*.sh' -exec chmod +x {} +
bash "$PROJECT_ROOT/scripts/aws/render_env.sh"
# El contenedor de Airflow corre con uid 1000 (usuario ubuntu) y escribe en src/dbt/target.
chown -R 1000:0 "$PROJECT_ROOT"

"$PROJECT_ROOT/scripts/start_infrastructure.sh"

compose() { docker compose --project-directory "$PROJECT_ROOT" -f "$PROJECT_ROOT/docker/docker-compose.yml" "$@"; }
for _ in {1..60}; do
  if compose exec -T airflow-scheduler airflow jobs check --job-type SchedulerJob >/dev/null 2>&1; then
    break
  fi
  sleep 5
done

# Fuentes iniciales en RDS y DocumentDB. Se reintenta mientras los endpoints terminan de estar listos.
if [[ ! -f "$PROJECT_ROOT/.sources_initialized" ]]; then
  for attempt in {1..10}; do
    if "$PROJECT_ROOT/scripts/generate_data.sh"; then
      touch "$PROJECT_ROOT/.sources_initialized"
      break
    fi
    printf 'generate_data failed (attempt %s); retrying in 30s...\n' "$attempt"
    sleep 30
  done
fi
[[ -f "$PROJECT_ROOT/.sources_initialized" ]] || { printf 'Error: could not load the initial sources.\n' >&2; exit 1; }

for _ in {1..30}; do
  compose exec -T airflow-scheduler airflow dags unpause ecommerce_pipeline >/dev/null 2>&1 && break
  sleep 5
done

touch "$PROJECT_ROOT/.bootstrap_complete"
printf 'DE-DW bootstrap complete. Airflow UI on port 8080.\n'
