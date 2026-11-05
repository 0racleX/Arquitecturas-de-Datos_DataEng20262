#!/usr/bin/env bash
# Ejecuta el flujo dbt completo contra Redshift fuera de Airflow (diagnóstico).
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
require_docker
require_env_file
printf 'Installing locked dbt packages...\n'
dbt_command deps
printf 'Loading seeds...\n'
dbt_command seed
printf 'Building intermediate models...\n'
dbt_command run --select path:models/intermediate
printf 'Building quarantine models...\n'
dbt_command run --select path:models/quarantine
printf 'Updating snapshots...\n'
dbt_command snapshot
printf 'Building warehouse models...\n'
dbt_command run --select path:models/marts
printf 'Running dbt data tests...\n'
dbt_command test
printf 'dbt workflow completed successfully.\n'
