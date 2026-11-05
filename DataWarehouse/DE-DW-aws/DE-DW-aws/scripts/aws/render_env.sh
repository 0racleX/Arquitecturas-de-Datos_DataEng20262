#!/usr/bin/env bash
# [EC2] Genera /opt/de-dw/.env desde Secrets Manager usando el rol IAM de la instancia.
# El archivo queda con permisos 600 y nunca se versiona.
set -euo pipefail
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck disable=SC1091
source /etc/de-dw.env

secret_json="$(aws secretsmanager get-secret-value \
  --secret-id "$DE_DW_SECRET_ID" --region "$AWS_REGION" \
  --query SecretString --output text)"

umask 077
jq -r --arg q "'" 'to_entries[] | "\(.key)=\($q)\(.value)\($q)"' <<< "$secret_json" > "$PROJECT_ROOT/.env"
chown 1000:0 "$PROJECT_ROOT/.env"
chmod 600 "$PROJECT_ROOT/.env"
printf 'Wrote %s/.env from secret %s\n' "$PROJECT_ROOT" "$DE_DW_SECRET_ID"
