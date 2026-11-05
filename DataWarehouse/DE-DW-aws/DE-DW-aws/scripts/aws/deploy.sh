#!/usr/bin/env bash
# [WSL] Crea o actualiza toda la infraestructura y espera a que la EC2 termine su arranque.
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/tf.sh"

for tool in terraform aws ssh; do
  command -v "$tool" >/dev/null 2>&1 || { printf 'Error: %s is not installed in WSL.\n' "$tool" >&2; exit 1; }
done
aws sts get-caller-identity --query 'Arn' --output text

# Quita saltos de línea de Windows (\r) del código que se sube a la EC2.
find "$PROJECT_ROOT/scripts" "$PROJECT_ROOT/src" "$PROJECT_ROOT/docker" -type f \
  \( -name '*.sh' -o -name '*.py' -o -name '*.yml' -o -name '*.sql' -o -name 'Dockerfile' \) -exec sed -i 's/\r$//' {} +
sed -i 's/\r$//' "$TF_DIR/user_data.sh.tftpl"
find "$PROJECT_ROOT/scripts" -name '*.sh' -exec chmod +x {} +

[[ -f "$TF_DIR/terraform.tfvars" ]] || cp "$TF_DIR/terraform.tfvars.example" "$TF_DIR/terraform.tfvars"
terraform -chdir="$TF_DIR" init -upgrade
terraform -chdir="$TF_DIR" apply "$@"

printf '\nWaiting for the EC2 bootstrap (Docker build + initial sources, ~10-15 min)...\n'
for _ in {1..120}; do
  if ec2_ssh 'test -f /opt/de-dw/.bootstrap_complete' 2>/dev/null; then
    printf '\nReady.\n  Airflow: %s\n  User:    airflow\n  Password: terraform -chdir=infra/terraform output -raw airflow_admin_password\n' "$(tf_output airflow_url)"
    exit 0
  fi
  sleep 15
done
printf 'Bootstrap still running. Follow it with: scripts/aws/ssh.sh sudo tail -f /var/log/de-dw-bootstrap.log\n'
