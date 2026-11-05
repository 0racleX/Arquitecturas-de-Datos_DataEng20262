#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
require_docker
require_env_file
printf 'Stopping Airflow services (metadata volume is preserved)...\n'
compose down
printf 'Airflow stopped.\n'
