#!/usr/bin/env bash
set -euo pipefail

# run_migrations.sh — Apply all database migrations in order
# Usage: ./run_migrations.sh [psql connection string]
# Default: psql postgresql://bornemap:bornemap_dev@localhost:5432/platform_db

PG_URL="${1:-postgresql://bornemap:bornemap_dev@localhost:5432/platform_db}"
DIR="$(cd "$(dirname "$0")" && pwd)/postgres"

echo "[migrate] running migrations against $PG_URL"

for f in "$DIR/migrations/"*.sql; do
  name=$(basename "$f")
  echo "[migrate] applying $name ..."
  psql "$PG_URL" -f "$f" -q 2>&1 | grep -v "already exists" || true
done

for f in "$DIR/seeds/"*.sql; do
  name=$(basename "$f")
  echo "[migrate] seeding $name ..."
  psql "$PG_URL" -f "$f" -q 2>&1 | grep -v "already exists" || true
done

echo "[migrate] complete"
