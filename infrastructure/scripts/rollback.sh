#!/usr/bin/env bash
set -euo pipefail

DEPLOY_HOST="${DEPLOY_HOST:?}"
DEPLOY_USER="${DEPLOY_USER:?}"
IMAGE_TAG="${1:?Usage: $0 <image-tag>}"

echo "=== Rolling back to $IMAGE_TAG ==="

ssh "$DEPLOY_USER@$DEPLOY_HOST" bash -s <<EOF
  set -euo pipefail
  cd /opt/bornemap
  export IMAGE_TAG=$IMAGE_TAG
  docker compose up -d --remove-orphans
  echo "Rollback to $IMAGE_TAG complete"
EOF
