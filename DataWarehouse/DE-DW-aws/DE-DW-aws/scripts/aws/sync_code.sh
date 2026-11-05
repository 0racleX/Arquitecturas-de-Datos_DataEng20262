#!/usr/bin/env bash
# [WSL] Copia tu código local a la EC2 (conserva .env) y reconstruye Airflow.
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/tf.sh"
tar -C "$PROJECT_ROOT" \
  --exclude='./.git' --exclude='./.env' --exclude='./infra/terraform/.terraform' \
  --exclude='./infra/terraform/.build' --exclude='./infra/terraform/*.tfstate*' \
  --exclude='./infra/terraform/*.pem' --exclude='./infra/terraform/terraform.tfvars' \
  --exclude='./src/dbt/target' --exclude='./src/dbt/dbt_packages' --exclude='./src/dbt/logs' \
  --exclude='__pycache__' --exclude='./evidencias' \
  -czf - . | ec2_ssh 'sudo tar -xzf - -C /opt/de-dw && sudo find /opt/de-dw -type f \( -name "*.sh" -o -name "*.py" -o -name "*.yml" -o -name "*.sql" -o -name "*.csv" -o -name Dockerfile \) -exec sed -i "s/\r$//" {} + && sudo find /opt/de-dw/scripts -name "*.sh" -exec chmod +x {} + && sudo chown -R 1000:0 /opt/de-dw && cd /opt/de-dw && sudo ./scripts/start_infrastructure.sh'
