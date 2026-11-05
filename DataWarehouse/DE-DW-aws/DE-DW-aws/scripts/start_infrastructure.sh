#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
require_docker
require_env_file
printf 'Building the Airflow image and starting Airflow on this EC2 instance...\n'
compose up --build -d
compose ps
