#!/bin/bash
# PostgreSQL docker-entrypoint-initdb.d wrapper
# Runs all .sql files from subdirectories in numeric order.
# Also creates keycloak_db for the Keycloak identity provider.

set -euo pipefail

echo "[init.sh] Running nested migrations..."

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Create keycloak_db if it doesn't exist
echo "[init.sh] Ensuring keycloak_db exists..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -c "SELECT 'CREATE DATABASE keycloak_db' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'keycloak_db')\gexec"

# Process all migration directories in order
for dir in gis ev users; do
  dir_path="$SCRIPT_DIR/$dir"
  if [ -d "$dir_path" ]; then
    echo "[init.sh] Processing $dir migrations..."
    for f in $(find "$dir_path" -name '*.sql' | sort); do
      echo "[init.sh]   Running: $f"
      psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f "$f"
    done
  fi
done

echo "[init.sh] All migrations complete."
