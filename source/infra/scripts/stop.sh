#!/usr/bin/env bash
set -euo pipefail

echo "==> BorneMap — Stopping infrastructure stack"

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_DIR="$(dirname "$DIR")"

cd "$COMPOSE_DIR"

echo "==> Stopping all containers"
docker compose down

REMAINING=$(docker compose ps --format json 2>/dev/null | grep -c '"ID"' 2>/dev/null || true)
if [ "$REMAINING" -gt 0 ] 2>/dev/null; then
  echo "WARNING: $REMAINING containers still running"
  docker compose ps
else
  echo "==> All containers stopped. No orphan processes."
fi

echo "==> Done"
