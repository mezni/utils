#!/bin/bash
set -euo pipefail

DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-bornemap}"
DB_USER="${DB_USER:-bornemap}"
DB_PASSWORD="${DB_PASSWORD:-bornemap}"

if [ ! -f /data/tunisia.osm.pbf ]; then
    echo "Downloading Tunisia OSM dataset..."
    mkdir -p /data
    wget -q --show-progress \
        -O /data/tunisia.osm.pbf \
        "https://download.geofabrik.de/africa/tunisia-latest.osm.pbf"
fi

echo "Extracting and importing EV charging stations..."
python3 scripts/parse_and_import.py \
    --pbf /data/tunisia.osm.pbf \
    --db-host "${DB_HOST}" \
    --db-port "${DB_PORT}" \
    --db-name "${DB_NAME}" \
    --db-user "${DB_USER}" \
    --db-password "${DB_PASSWORD}"

echo "Import complete."
