#!/usr/bin/env bash
# Deja el laboratorio como recién desplegado: borra los esquemas del warehouse en Redshift,
# los lotes raw de S3 (con --purge-s3) y regenera las fuentes. La infraestructura sigue en pie.
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
require_docker
load_env_file

purge_s3=false
[[ "${1:-}" == "--purge-s3" ]] && purge_s3=true

printf 'Dropping Redshift schemas raw_data, intermediate, quarantine, snapshots, marts and ops...\n'
airflow_python -m ecommerce_dw.ops.evidence reset-warehouse

if [[ "$purge_s3" == true ]]; then
  printf 'Deleting s3://%s/raw/ and s3://%s/control/ ...\n' "$DE_DW_BUCKET" "$DE_DW_BUCKET"
  aws s3 rm "s3://$DE_DW_BUCKET/raw/" --recursive --region "$AWS_REGION"
  aws s3 rm "s3://$DE_DW_BUCKET/control/" --recursive --region "$AWS_REGION"
fi

rm -rf "$PROJECT_ROOT/src/dbt/target" "$PROJECT_ROOT/src/dbt/logs" "$PROJECT_ROOT/src/dbt/dbt_packages"
"$PROJECT_ROOT/scripts/generate_data.sh"
printf 'Reset complete. Run scripts/run_pipeline.sh --mode full for a new initial load.\n'
