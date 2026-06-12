#!/usr/bin/env bash
# generate-load-data.sh — Generate 1000 stations with random Tunisia coordinates
# Usage: scripts/generate-load-data.sh [NUM_STATIONS]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$PROJECT_DIR/.env"

NUM_STATIONS="${1:-1000}"

if [ ! -f "$ENV_FILE" ]; then
  echo "[ERROR] No .env file found."
  exit 1
fi

set -a
source "$ENV_FILE"
set +a

echo "[INFO] Generating $NUM_STATIONS load-test stations ..."

psql "$PLATFORM_DB_URL" <<SQL
DO \$\$
DECLARE
  i INT;
  lat DOUBLE PRECISION;
  lng DOUBLE PRECISION;
  station_id VARCHAR(50);
BEGIN
  FOR i IN 1..$NUM_STATIONS LOOP
    lat := 30.0 + random() * 7.0;
    lng := 7.5 + random() * 5.0;
    station_id := 'STA-load-' || LPAD(i::TEXT, 6, '0');

    INSERT INTO inventory.station (id, name, address, lat, lng, status, partner_id)
    VALUES (
      station_id,
      'Load-Test Station ' || i,
      'Load Test Address ' || i,
      lat,
      lng,
      CASE (random() * 3)::INT
        WHEN 0 THEN 'available'
        WHEN 1 THEN 'busy'
        WHEN 2 THEN 'offline'
        ELSE 'unknown'
      END,
      'PRT-totalenergies-tn'
    )
    ON CONFLICT (id) DO NOTHING;
  END LOOP;
END;
\$\$;

SELECT COUNT(*) AS total_stations FROM inventory.station;
SQL

echo "[DONE] Load data generated."
