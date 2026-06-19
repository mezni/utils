#!/usr/bin/env bash
set -euo pipefail

echo "==> BorneMap — Starting infrastructure stack"

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_DIR="$(dirname "$DIR")"

cd "$COMPOSE_DIR"

# Ensure .env exists
if [ ! -f .env ]; then
  echo "ERROR: .env file not found. Copy .env.example to .env and fill in values."
  exit 1
fi

echo "==> Starting Docker Compose"
docker compose up -d

echo "==> Waiting for all containers to become healthy..."
TIMEOUT=120
INTERVAL=5
ELAPSED=0

while [ $ELAPSED -lt $TIMEOUT ]; do
  ALL_HEALTHY=true
  for SERVICE in postgres redis keycloak traefik; do
    STATUS=$(docker compose ps --format json "$SERVICE" 2>/dev/null | grep -o '"Status":"[^"]*"' | head -1 | cut -d'"' -f4)
    if [ "$STATUS" != "running" ] && [ "$STATUS" != "healthy" ]; then
      ALL_HEALTHY=false
      break
    fi
    # Check health label
    HEALTH=$(docker inspect --format='{{.State.Health.Status}}' "bornemap-$SERVICE" 2>/dev/null || echo "")
    if [ "$HEALTH" != "healthy" ] && [ -n "$HEALTH" ]; then
      ALL_HEALTHY=false
      break
    fi
  done

  if [ "$ALL_HEALTHY" = true ]; then
    echo "==> All containers healthy!"
    echo "    Postgres: :5432"
    echo "    Redis:    :6379"
    echo "    Keycloak: :8080"
    echo "    Traefik:  :80"
    docker compose ps
    exit 0
  fi

  sleep "$INTERVAL"
  ELAPSED=$((ELAPSED + INTERVAL))
  echo "    Waiting... (${ELAPSED}s / ${TIMEOUT}s)"
done

echo "ERROR: Timed out after ${TIMEOUT}s waiting for containers to become healthy."
docker compose ps
exit 1
