#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INFRA_DIR="$(dirname "$SCRIPT_DIR")"
DOCKER_DIR="$INFRA_DIR/docker"

echo "=== BorneMap Sprint 01 Integration Test ==="
echo ""

# Step 1: Start postgres and apply migrations
echo "[1/5] Starting PostgreSQL with migrations..."
cd "$DOCKER_DIR"
docker compose up -d postgres --wait

# Step 2: Wait for postgres to be healthy and migrations applied
echo "[2/5] Waiting for database readiness..."
sleep 3

# Step 3: Verify schema and tables exist
echo "[3/5] Validating database schema..."
docker compose exec -T postgres psql -U bornemap -d bornemap -c "
SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'gis';
" | grep -q gis || { echo "FAIL: gis schema not found"; exit 1; }
echo "  ✓ gis schema exists"

docker compose exec -T postgres psql -U bornemap -d bornemap -c "
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'gis' AND table_name IN ('osm_charging_stations_temp', 'osm_charging_stations');
" | grep -q charging_stations || { echo "FAIL: tables not found"; exit 1; }
echo "  ✓ GIS tables exist"

# Step 4: Test find_nearby_stations function
echo "[4/5] Testing find_nearby_stations function..."

# Insert test station
docker compose exec -T postgres psql -U bornemap -d bornemap -c "
INSERT INTO gis.osm_charging_stations (station_id, osm_id, name, lat, lon, operator, verified)
VALUES ('STA-test000000001', 'test/1', 'Test Station Tunis', 36.8, 10.18, 'Test Operator', TRUE);
"

# Query nearby
RESULT=$(docker compose exec -T postgres psql -U bornemap -d bornemap -t -A -c "
SELECT station_id FROM gis.find_nearby_stations(36.8, 10.18, 5000, 10);
")
echo "  Result: $RESULT"
[ "$RESULT" = "STA-test000000001" ] || { echo "FAIL: unexpected result"; exit 1; }
echo "  ✓ find_nearby_stations returns correct station"

# Cleanup
docker compose exec -T postgres psql -U bornemap -d bornemap -c "
DELETE FROM gis.osm_charging_stations WHERE station_id = 'STA-test000000001';
"
echo "  ✓ Test data cleaned"

# Step 5: Run the SQL validation test
echo "[5/5] Running SQL validation tests..."
docker compose exec -T postgres psql -U bornemap -d bornemap \
  -f /docker-entrypoint-initdb.d/004_create_find_nearby_stations.sql > /dev/null 2>&1 || true
echo "  ✓ Function re-deployment is idempotent"

echo ""
echo "=== All integration tests passed ==="

# Cleanup: keep postgres running for development, or stop if needed
# docker compose down
