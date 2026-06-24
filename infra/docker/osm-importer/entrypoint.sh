#!/bin/sh
set -e

echo "=== OSM Importer Starting ==="
echo "Postgres Host: ${PGHOST:-postgres}"
echo "Postgres DB:   ${PGDATABASE:-bornemap}"
echo "OSM URL:       ${OSM_URL}"

python /app/import_osm.py

echo "=== OSM Importer Completed ==="
