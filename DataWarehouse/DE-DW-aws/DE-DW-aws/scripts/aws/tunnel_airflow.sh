#!/usr/bin/env bash
# [WSL] Alternativa si cambia tu IP: túnel SSH a Airflow en http://localhost:8080
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/tf.sh"
printf 'Open http://localhost:8080 (Ctrl+C to close the tunnel)\n'
ec2_ssh -N -L 8080:localhost:8080
