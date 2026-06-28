#!/usr/bin/env bash
set -euo pipefail

DB_URL="${DATABASE_URL:-postgres://bornemap:bornemap_dev@localhost:5432/bornemap}"

echo "Running database migrations..."
echo "Database URL: $DB_URL"

cargo install sqlx-cli --no-default-features --features postgres 2>/dev/null || true
sqlx migrate run --database-url "$DB_URL" --source database/migrations

echo "Migrations complete."
