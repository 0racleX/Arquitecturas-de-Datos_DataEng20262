#!/usr/bin/env bash
# Consultas de evidencia contra Redshift (se ejecutan dentro del contenedor del scheduler).
#   ./scripts/warehouse_report.sh all|batches|raw|quarantine|facts|scd2|reconcile|dashboard|state
#   ./scripts/warehouse_report.sh sql "select count(*) from marts.fct_sales"
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
require_docker
require_env_file
airflow_python -m ecommerce_dw.ops.evidence "${@:-all}"
