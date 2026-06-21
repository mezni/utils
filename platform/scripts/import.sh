#!/usr/bin/env bash
set -euo pipefail

# import.sh — Fetch OSM charging stations for Tunisia and insert into staging
# Uses Overpass API with hardcoded Tunisia bounding box

PG_URL="${1:-postgresql://bornemap:bornemap_dev@localhost:5432/platform_db}"
BBOX="${2:-30.0,7.0,38.0,12.0}"  # Tunisia: south,west,north,east

echo "[import] fetching Tunisia charging stations from Overpass API ..."

OVERPASS_QUERY="[out:json];node[amenity=charging_station]($BBOX);out;"

RESPONSE=$(curl -s -X POST -d "data=$OVERPASS_QUERY" \
  "https://overpass-api.de/api/interpreter") || {
  echo "[import] ERROR: Overpass API request failed" >&2
  exit 1
}

COUNT=$(echo "$RESPONSE" | jq '.elements | length')
echo "[import] found $COUNT stations"

echo "$RESPONSE" | jq -c '.elements[]' | while read -r element; do
  osm_id=$(echo "$element" | jq '.id')
  lat=$(echo "$element" | jq '.lat')
  lng=$(echo "$element" | jq '.lon')
  tags=$(echo "$element" | jq -c '.tags // {}' | psql "$PG_URL" -q -t -c "SELECT hstore('\"' || array_to_string(array_agg(key || '=>' || replace(value, '\"', '\\\\\"')), '\", \"') || '\"') FROM (SELECT key, value FROM jsonb_each_text('\$1'::jsonb)) t;" 2>/dev/null || echo "''::hstore")

  psql "$PG_URL" -q -c "
    INSERT INTO gis.osm_charging_stations_temp (osm_id, lat, lng, raw_tags)
    VALUES ($osm_id, $lat, $lng, '$tags')
    ON CONFLICT (osm_id) DO NOTHING;" 2>/dev/null
done

echo "[import] complete — $COUNT stations staged"
