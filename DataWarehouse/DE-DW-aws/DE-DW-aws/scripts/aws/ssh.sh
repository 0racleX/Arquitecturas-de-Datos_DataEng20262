#!/usr/bin/env bash
# [WSL] Abre una sesión SSH en la EC2, o ejecuta un comando: scripts/aws/ssh.sh 'docker ps'
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/tf.sh"
ec2_ssh "$@"
