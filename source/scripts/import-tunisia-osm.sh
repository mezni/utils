#!/usr/bin/env bash
set -euo pipefail

DB_URL="${DATABASE_URL:-postgres://bornemap:bornemap@localhost:5432/bornemap_platform}"

OVERPASS_URL="https://overpass-api.de/api/interpreter"
TUNISIA_BBOX="30.0,7.0,38.0,12.0"

QUERY=$(cat <<'EOF'
[out:json][timeout:90];
(
  node["amenity"="charging_station"](30.0,7.0,38.0,12.0);
  way["amenity"="charging_station"](30.0,7.0,38.0,12.0);
);
out center;
EOF
)

echo "Fetching Tunisia EV charging stations from Overpass API..."
RESPONSE=$(curl -s -d "${QUERY}" "${OVERPASS_URL}")

echo "Parsing response..."
echo "${RESPONSE}" | jq -c '.elements[] | select(.type == "node")' | while read -r element; do
    ID=$(echo "${element}" | jq -r '.id')
    LAT=$(echo "${element}" | jq -r '.lat')
    LON=$(echo "${element}" | jq -r '.lon')
    NAME=$(echo "${element}" | jq -r '.tags.name // "Charging Station"')
    ADDRESS=$(echo "${element}" | jq -r '.tags."addr:full" // .tags."addr:street" // ""')

    INSERTED=$(psql "${DB_URL}" -t -c "
        INSERT INTO gis.osm_stations (id, name, address, coordinates, source, is_available)
        VALUES (
            'OSM-${ID}',
            '${NAME}',
            '${ADDRESS}',
            ST_SetSRID(ST_MakePoint(${LON}, ${LAT}), 4326),
            'OSM_IMPORT',
            TRUE
        )
        ON CONFLICT (id) DO UPDATE
        SET name = EXCLUDED.name,
            address = EXCLUDED.address,
            coordinates = EXCLUDED.coordinates,
            is_available = TRUE,
            last_modified_at = CURRENT_TIMESTAMP;
    " 2>&1)

    if [ $? -eq 0 ]; then
        echo "  Imported station: ${NAME} (${LAT}, ${LON})"
    else
        echo "  Failed to import station ${ID}: ${INSERTED}" >&2
    fi
done

echo "Import complete."
