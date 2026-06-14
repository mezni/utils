#!/usr/bin/env bash

# ============================================================================
# 🗺️ BORNE MAP TUNISIAN OSM INFRASTRUCTURE INGESTION PIPELINE
# ============================================================================
set -euo pipefail

# 1. Pipeline Environment Parameters
POSTGRES_CONTAINER="bornemap-postgres"
DB_USER="platform_admin"
DB_NAME="platform_db"

# Strict bounding box framing Tunisia's operational infrastructure perimeter
# Syntax: (south_lat, west_lon, north_lat, east_lon)
TUNISIA_BBOX="30.0,7.0,38.0,12.0"

# Target query identifying public EV charging nodes
OVERPASS_QUERY="[out:json][timeout:90];node[\"amenity\"=\"charging_station\"](${TUNISIA_BBOX});out body;"
OVERPASS_API_URL="https://overpass-api.de/api/interpreter"

echo "========================================================================="
echo "🔌 Starting BorneMap Spatial Data Ingestion from OpenStreetMap..."
echo "========================================================================="

# 2. Verify Database Container Connectivity
if ! docker exec "$POSTGRES_CONTAINER" pg_isready -U "$DB_USER" -d "$DB_NAME" > /dev/null 2>&1; then
    echo "❌ Error: Target database container '$POSTGRES_CONTAINER' is offline or unreachable."
    exit 1
fi

echo "✅ Database connection verified"

# 3. Pull Live Geospatial Assets from Overpass Server API
echo "🛰️  Querying Overpass API for Tunisian EV Charger Nodes..."
RAW_RESPONSE=$(curl -s --speed-time 15 --speed-limit 1 \
    --data-urlencode "data=${OVERPASS_QUERY}" \
    "$OVERPASS_API_URL")

# Check if payload contains actual data nodes
NODE_COUNT=$(echo "$RAW_RESPONSE" | grep -c '"type": "node"' || true)
if [ "$NODE_COUNT" -eq 0 ]; then
    echo "⚠️  Anomalous State: Overpass returned zero charging nodes inside the Tunisian perimeter."
    echo "This is expected if the query is running for the first time or there are network issues."
    exit 0
fi

echo "✅ Successfully downloaded $NODE_COUNT geospatial charging points."

# 4. Stream and Map Payloads into PostGIS Spatial Layer Cache
echo "💾 Streaming structured payloads into 'gis.osm_stations' cache layer..."

# We use standard awk/sed string matching to format the data into clean SQL inserts,
# keeping the pipeline fast, lightweight, and free from heavy node dependencies.
echo "$RAW_RESPONSE" | awk -v count="$NODE_COUNT" '
BEGIN {
    FS="[:,]"
    print "BEGIN;"
}
/\"id\":/   { id = $2; gsub(/[ "]/, "", id); id = "osm-" id }
/\"lat\":/  { lat = $2; gsub(/[ "]/, "", lat) }
/\"lon\":/  { lon = $2; gsub(/[ "]/, "", lon) }
/\"name\":/ { 
    split($0, arr, "\"name\": \"")
    split(arr[2], name_part, "\"")
    name = name_part[1]
    gsub(/'\''/, "'\'''\''", name) 
}
/\"operator\":/ {
    split($0, arr, "\"operator\": \"")
    split(arr[2], op_part, "\"")
    operator = op_part[1]
    gsub(/'\''/, "'\'''\''", operator)
}
/}/ {
    if (id != "" && lat != "" && lon != "") {
        if (name == "") {
            if (operator != "") name = operator " Station"
            else name = "Public OSM EV Charger"
        }
        
        printf "INSERT INTO gis.osm_stations (id, name, address, coordinates, source, is_available) VALUES ('\''%s'\'', '\''%s'\'', NULL, ST_SetSRID(ST_MakePoint(%s, %s), 4326), '\''OSM_IMPORT'\'', TRUE) ON CONFLICT (id) DO NOTHING;\n", id, name, lon, lat
        
        id = ""; lat = ""; lon = ""; name = ""; operator = ""
    }
}
END {
    print "COMMIT;"
}
' | docker exec -i "$POSTGRES_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" > /dev/null

echo "🎉 Data ingestion pipeline completed successfully!"
echo "========================================================================="
