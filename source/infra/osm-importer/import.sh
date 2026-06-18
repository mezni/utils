#!/bin/bash
set -euo pipefail

# BorneMap OSM Importer
# Downloads Tunisia OSM data from Geofabrik and imports into PostGIS
#
# Environment variables:
#   DB_HOST      — PostGIS host (default: localhost)
#   DB_PORT      — PostGIS port (default: 5432)
#   DB_USER      — database user (default: bornemap)
#   DB_PASSWORD  — database password (default: bornemap_dev)
#   DB_NAME      — database name (default: platform_db)
#   PBF_URL      — OSM extract URL (default: Tunisia)
#   OSM_FILE     — PBF filename (default: tunisia-latest.osm.pbf)

DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_USER="${DB_USER:-bornemap}"
DB_PASSWORD="${DB_PASSWORD:-bornemap_dev}"
DB_NAME="${DB_NAME:-platform_db}"
PBF_URL="${PBF_URL:-https://download.geofabrik.de/africa/tunisia-latest.osm.pbf}"
OSM_FILE="${OSM_FILE:-tunisia-latest.osm.pbf}"
STYLE_FILE="/osm2pgsql.style"

export PGPASSWORD="$DB_PASSWORD"

echo "=== BorneMap OSM Importer ==="
echo "Target:  $DB_HOST:$DB_PORT / $DB_NAME"
echo "DB user: $DB_USER"

# --- Download ---
if [ ! -f "$OSM_FILE" ]; then
    echo "=== Downloading $PBF_URL ==="
    wget --progress=bar:force "$PBF_URL" -O "$OSM_FILE"
    echo "Download complete: $(ls -lh "$OSM_FILE" | awk '{print $5}')"
else
    echo "File $OSM_FILE already exists, skipping download"
fi

# --- Import ---
echo "=== Importing with osm2pgsql ==="
osm2pgsql \
    -H "$DB_HOST" \
    -P "$DB_PORT" \
    -U "$DB_USER" \
    -d "$DB_NAME" \
    --schema gis \
    --prefix osm \
    --create \
    --latlong \
    --hstore \
    "$OSM_FILE"

echo "=== Creating spatial indexes ==="
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" <<-EOSQL
    CREATE INDEX IF NOT EXISTS idx_osm_roads_geom
        ON gis.osm_roads USING GIST (way);
    CREATE INDEX IF NOT EXISTS idx_osm_point_geom
        ON gis.osm_point USING GIST (way);
    CREATE INDEX IF NOT EXISTS idx_osm_polygon_geom
        ON gis.osm_polygon USING GIST (way);
EOSQL

echo "=== Import complete ==="
