#!/usr/bin/env bash
# init-dbs.sh — Run all database migrations
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
MIGRATIONS_DIR="$PROJECT_DIR/infra/migrations"
ENV_FILE="$PROJECT_DIR/.env"

if [ ! -f "$ENV_FILE" ]; then
  echo "[ERROR] No .env file found. Run dev.sh first or create one from infra/.env.example"
  exit 1
fi

set -a
source "$ENV_FILE"
set +a

run_migration() {
  local db_url="$1"
  local file="$2"
  local label="$3"

  echo "[MIGRATE] $label ($(basename "$file"))"
  psql "$db_url" -q -f "$file"
  echo "[OK]     $label"
}

# Phase 1: platform_db migrations (001, 002, 003)
run_migration "$PLATFORM_DB_URL" "$MIGRATIONS_DIR/001-platform-db-init.sql" "platform_db: init schemas + PostGIS"

if [ -f "$MIGRATIONS_DIR/002-inventory-schema.sql" ]; then
  run_migration "$PLATFORM_DB_URL" "$MIGRATIONS_DIR/002-inventory-schema.sql" "platform_db: inventory schema"
fi

if [ -f "$MIGRATIONS_DIR/003-gis-schema.sql" ]; then
  run_migration "$PLATFORM_DB_URL" "$MIGRATIONS_DIR/003-gis-schema.sql" "platform_db: gis schema"
fi

# Phase 2: analytics_db migration (004)
run_migration "$ANALYTICS_DB_URL" "$MIGRATIONS_DIR/004-analytics-db-init.sql" "analytics_db: raw_events + append-only rules"

# Phase 3: seed data (005) — platform_db only
if [ -f "$MIGRATIONS_DIR/005-seed-data.sql" ]; then
  run_migration "$PLATFORM_DB_URL" "$MIGRATIONS_DIR/005-seed-data.sql" "platform_db: seed data"
fi

echo "[DONE] All migrations complete."
