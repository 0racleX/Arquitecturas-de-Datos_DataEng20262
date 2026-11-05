#!/usr/bin/env bash
# [WSL] Utilidades compartidas por los scripts que corren en tu máquina.
set -euo pipefail
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TF_DIR="$PROJECT_ROOT/infra/terraform"

tf_output() {
  terraform -chdir="$TF_DIR" output -raw "$1"
}

ec2_ssh() {
  local ip key
  ip="$(tf_output ec2_public_ip)"
  key="$(tf_output ssh_private_key_path)"
  chmod 600 "$key"
  ssh -i "$key" -o StrictHostKeyChecking=accept-new -o ServerAliveInterval=30 "ubuntu@$ip" "$@"
}
