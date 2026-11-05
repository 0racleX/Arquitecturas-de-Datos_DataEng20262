#!/usr/bin/env bash
# [WSL] Elimina TODOS los recursos (EC2, RDS, DocumentDB, Redshift, S3 con su contenido, secretos).
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/tf.sh"
terraform -chdir="$TF_DIR" destroy "$@"
printf 'Remember: the QuickSight subscription (if you created it in the console) is not managed by Terraform.\n'
