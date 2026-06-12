#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="$PROJECT_DIR/infra/docker-compose.yml"
ENV_FILE="$PROJECT_DIR/.env"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC}  $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

if [ ! -f "$ENV_FILE" ]; then
  log_warn "No .env file found at $ENV_FILE"
  log_info "Copying from infra/.env.example ..."
  cp "$PROJECT_DIR/infra/.env.example" "$ENV_FILE"
  log_info "Created $ENV_FILE — review and adjust before proceeding."
fi

set -a
source "$ENV_FILE"
set +a

cleanup() {
  log_info "Shutting down services ..."
  docker compose -f "$COMPOSE_FILE" down
}
trap cleanup EXIT

log_info "Starting BorneMap services ..."
docker compose -f "$COMPOSE_FILE" up -d

log_info "Waiting for platform-db health check ..."
if ! docker compose -f "$COMPOSE_FILE" exec -T platform-db \
  pg_isready -U "$PLATFORM_DB_USER" -d platform_db -q; then
  log_error "platform-db failed to become healthy."
  exit 1
fi

log_info "Waiting for analytics-db health check ..."
if ! docker compose -f "$COMPOSE_FILE" exec -T analytics-db \
  pg_isready -U "$ANALYTICS_DB_USER" -d analytics_db -q; then
  log_error "analytics-db failed to become healthy."
  exit 1
fi

log_info "Both databases are healthy."

SCRIPT_DIR="$SCRIPT_DIR" "$SCRIPT_DIR/init-dbs.sh"

log_info "Validating schemas ..."
docker compose -f "$COMPOSE_FILE" exec -T platform-db \
  psql -U "$PLATFORM_DB_USER" -d platform_db -c "
    SELECT schema_name FROM information_schema.schemata
    WHERE schema_name IN ('inventory','gis');
  " | grep -q inventory || {
  log_error "platform_db: inventory schema missing."
  exit 1
}
docker compose -f "$COMPOSE_FILE" exec -T analytics-db \
  psql -U "$ANALYTICS_DB_USER" -d analytics_db -c "
    SELECT table_name FROM information_schema.tables
    WHERE table_name = 'raw_events';
  " | grep -q raw_events || {
  log_error "analytics_db: raw_events table missing."
  exit 1
}

log_info "All services are ready."
log_info "  platform-db:  postgresql://$PLATFORM_DB_USER@localhost:$PLATFORM_DB_PORT/platform_db"
log_info "  analytics-db: postgresql://$ANALYTICS_DB_USER@localhost:$ANALYTICS_DB_PORT/analytics_db"
echo ""
echo "Press Ctrl+C to stop all services."
wait
