#!/usr/bin/env bash
# [WSL] Ejecuta un script del proyecto en la EC2, p. ej.:
#   scripts/aws/remote.sh run_pipeline.sh --mode full
#   scripts/aws/remote.sh warehouse_report.sh batches
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/tf.sh"
[[ "$#" -ge 1 ]] || { printf 'Usage: %s SCRIPT [ARGS...]\n' "$0" >&2; exit 2; }
script="$1"; shift
args=""
[[ "$#" -eq 0 ]] || args="$(printf '%q ' "$@")"
ec2_ssh -t "cd /opt/de-dw && sudo ./scripts/$script $args"
