#!/bin/bash
# PostgreSQL docker-entrypoint-initdb.d wrapper
# Runs all .sql files from subdirectories in numeric order.
# PostgreSQL's init system only processes files at the root level,
# so this script sources the nested migrations.

set -euo pipefail

echo "[init.sh] Running nested migrations..."

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

for dir in gis ev; do
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
