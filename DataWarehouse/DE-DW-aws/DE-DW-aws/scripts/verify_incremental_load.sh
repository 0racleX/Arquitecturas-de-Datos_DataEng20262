#!/usr/bin/env bash
# Verifica tres escenarios con un lote diario nuevo de las fuentes (append_batch.py --batch-id N):
#   1. Nuevo lote incremental: cada hecho crece exactamente 3 filas y concilia con su fuente validada.
#   2. Repetición de un lote exitoso: reprocesar el mismo batch_id desde S3 no cambia nada.
#   3. Reenvío del mismo lote de origen + nueva extracción: no se duplican hechos ni versiones SCD2.
#   ./scripts/verify_incremental_load.sh --batch-id N    (N sin usar, entre 1 y 365)
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
require_docker
require_env_file
[[ "$#" -eq 2 && "$1" == "--batch-id" && "$2" =~ ^[0-9]+$ ]] || {
  printf 'Usage: %s --batch-id UNUSED_NUMBER\n' "$0" >&2
  exit 2
}

source_batch="$2"
pipeline_batch="incremental_${source_batch}"
scripts_dir="$PROJECT_ROOT/scripts"

warehouse_state() {
  airflow_python -m ecommerce_dw.ops.evidence state
}

# Falla si un hecho no concilia con su fuente validada o tiene llaves duplicadas.
assert_reconciled() {
  local state="$1" failures
  failures="$(awk '
    { rows[$1] = $2; unique[$1] = $3; total[$1] = $4 }
    END {
      pairs["fct_sales"] = "valid_order_items"
      pairs["fct_reviews"] = "valid_reviews"
      pairs["fct_navigation_events"] = "valid_navigation_events"
      for (fact in pairs) {
        source = pairs[fact]
        if (rows[fact] != rows[source] || unique[fact] != rows[fact] || total[fact] + 0 != total[source] + 0) {
          print fact
        }
      }
    }' <<< "$state")"
  [[ -z "$failures" ]] || {
    printf 'Error: facts do not reconcile with their validated sources: %s\n%s\n' "$failures" "$state" >&2
    exit 1
  }
}

state_value() {
  awk -v name="$2" -v column="$3" '$1 == name { print $column }' <<< "$1"
}

baseline="$(warehouse_state)"
assert_reconciled "$baseline"
printf 'Baseline reconciles:\n%s\n' "$baseline"

first_append="$("$scripts_dir/append_batch.sh" --batch-id "$source_batch")"
printf '%s\n' "$first_append"
[[ "$first_append" == *"3 new orders"* ]] || {
  printf 'Error: source batch %s is already present. Choose an unused batch ID.\n' "$source_batch" >&2
  exit 1
}

printf '\n[1/3] New incremental batch %s\n' "$pipeline_batch"
"$scripts_dir/run_pipeline.sh" --batch-id "$pipeline_batch"
after_first="$(warehouse_state)"
assert_reconciled "$after_first"
for fact in fct_sales fct_reviews fct_navigation_events; do
  expected=$(( $(state_value "$baseline" "$fact" 2) + 3 ))
  [[ "$(state_value "$after_first" "$fact" 2)" -eq "$expected" ]] || {
    printf 'Error: %s did not grow by 3 rows:\n%s\n' "$fact" "$after_first" >&2
    exit 1
  }
done
printf 'Every fact grew by 3 rows and reconciles:\n%s\n' "$after_first"

printf '\n[2/3] Re-running successful batch %s (files reused from S3, sources not queried)\n' "$pipeline_batch"
"$scripts_dir/run_pipeline.sh" --batch-id "$pipeline_batch"
after_rerun="$(warehouse_state)"
[[ "$after_rerun" == "$after_first" ]] || {
  printf 'Error: re-running batch %s changed the warehouse:\n%s\n---\n%s\n' "$pipeline_batch" "$after_first" "$after_rerun" >&2
  exit 1
}
printf 'Re-run left facts and snapshots unchanged.\n'

printf '\n[3/3] Re-sending source batch %s and extracting a new pipeline batch\n' "$source_batch"
second_append="$("$scripts_dir/append_batch.sh" --batch-id "$source_batch")"
printf '%s\n' "$second_append"
[[ "$second_append" == *"0 new orders"* ]] || {
  printf 'Error: repeating source batch %s added orders.\n' "$source_batch" >&2
  exit 1
}
"$scripts_dir/run_pipeline.sh" --batch-id "${pipeline_batch}_repeat"
after_repeat="$(warehouse_state)"
[[ "$after_repeat" == "$after_first" ]] || {
  printf 'Error: repeating source batch %s changed the warehouse:\n%s\n---\n%s\n' "$source_batch" "$after_first" "$after_repeat" >&2
  exit 1
}
printf '\nIncremental load verified: new rows added once; re-runs left every fact and snapshot unchanged.\n'
