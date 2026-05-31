#!/usr/bin/env bash
set -euo pipefail

DEPLOY_HOST="${DEPLOY_HOST:?}"
DEPLOY_USER="${DEPLOY_USER:?}"
COMPOSE_FILE="infrastructure/compose/docker-compose.yml"

echo "=== Deploying BorneMap ==="

ssh "$DEPLOY_USER@$DEPLOY_HOST" bash -s <<EOF
  set -euo pipefail
  cd /opt/bornemap
  docker compose pull
  docker compose up -d --remove-orphans
  echo "Deploy complete"
EOF
