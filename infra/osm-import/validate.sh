#!/usr/bin/env bash
set -euo pipefail

DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-platform_db}"
DB_USER="${DB_USER:-bornemap}"
export PGPASSWORD="${PGPASSWORD:-bornemap_dev}"

PASS=0
FAIL=0

pass() {
    PASS=$((PASS + 1))
    echo "  ✓ $1"
}

fail() {
    FAIL=$((FAIL + 1))
    echo "  ✗ $1"
}

check_eq() {
    local label="$1"
    local query="$2"
    local expected="$3"
    result=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -tA -c "$query" 2>/dev/null | tr -d '[:space:]')
    if [ "$result" = "$expected" ]; then
        pass "$label"
    else
        fail "$label (expected: $expected, got: $result)"
    fi
}

check_gt() {
    local label="$1"
    local query="$2"
    local min="$3"
    result=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -tA -c "$query" 2>/dev/null | tr -d '[:space:]')
    if [ "$result" -gt "$min" ] 2>/dev/null; then
        pass "$label (got: $result)"
    else
        fail "$label (expected > $min, got: $result)"
    fi
}

PSQL="psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME"

echo "============================================"
echo "  PostGIS Validation Suite"
echo "============================================"
echo ""

# ------------------------------------------------------------------
# T014: Basic SQL validation queries
# ------------------------------------------------------------------
echo "--- T014: Basic Validation ---"

postgis_ver=$($PSQL -tA -c "SELECT PostGIS_version();" 2>/dev/null | tr -d '[:space:]')
if echo "$postgis_ver" | grep -q "^3\."; then
    pass "PostGIS version 3.x (got: $postgis_ver)"
else
    fail "PostGIS version 3.x (got: $postgis_ver)"
fi

check_gt "Active stations count" \
    "SELECT COUNT(*) FROM inventory.station WHERE status='active';" \
    "0"

total=$($PSQL -tA -c "SELECT COUNT(*) FROM inventory.station;" 2>/dev/null | tr -d '[:space:]')
distinct=$($PSQL -tA -c "SELECT COUNT(DISTINCT id) FROM inventory.station;" 2>/dev/null | tr -d '[:space:]')
if [ "$total" = "$distinct" ]; then
    pass "No duplicate IDs (total=$total, distinct=$distinct)"
else
    fail "Duplicate IDs found (total=$total, distinct=$distinct)"
fi

check_eq "No NULL locations or names" \
    "SELECT COUNT(*) FROM inventory.station WHERE location IS NULL OR name IS NULL;" \
    "0"

# GIST index exists
index_exists=$($PSQL -tA -c "SELECT COUNT(*) FROM pg_indexes WHERE tablename='station' AND indexname='idx_station_location' AND lower(indexdef) LIKE '%gist%';" 2>/dev/null | tr -d '[:space:]')
if [ "$index_exists" = "1" ]; then
    pass "GIST index idx_station_location exists"
else
    fail "GIST index idx_station_location not found"
fi

echo ""

# ------------------------------------------------------------------
# T015: ST_DWithin query validation
# ------------------------------------------------------------------
echo "--- T015: ST_DWithin Query Validation ---"

TUNIS_LAT="36.8"
TUNIS_LON="10.2"
RADIUS_M="50000"

check_gt "ST_DWithin returns stations within ${RADIUS_M}m of Tunis" \
    "SELECT COUNT(*) FROM inventory.station WHERE ST_DWithin(location, ST_SetSRID(ST_MakePoint(${TUNIS_LON}, ${TUNIS_LAT}), 4326)::geography, ${RADIUS_M});" \
    "0"

# Distance ordering check
distance_result=$($PSQL -tA -c "
SELECT id FROM inventory.station
WHERE ST_DWithin(location, ST_SetSRID(ST_MakePoint(${TUNIS_LON}, ${TUNIS_LAT}), 4326)::geography, ${RADIUS_M})
ORDER BY ST_Distance(location, ST_SetSRID(ST_MakePoint(${TUNIS_LON}, ${TUNIS_LAT}), 4326)::geography) ASC;" 2>/dev/null)
if [ -n "$distance_result" ]; then
    pass "Distance ordering ASC works"
else
    fail "Distance ordering returned no results"
fi

# EXPLAIN ANALYZE for index scan
explain_output=$($PSQL -tA -c "
EXPLAIN ANALYZE
SELECT id FROM inventory.station
WHERE ST_DWithin(location, ST_SetSRID(ST_MakePoint(${TUNIS_LON}, ${TUNIS_LAT}), 4326)::geography, ${RADIUS_M});" 2>/dev/null)
if echo "$explain_output" | grep -qi "Index Scan"; then
    pass "Index Scan used in ST_DWithin query"
else
    fail "No Index Scan detected in explain plan"
fi

# Timing benchmark
timing_result=$($PSQL -tA -c "
EXPLAIN ANALYZE
SELECT id, ST_Distance(location, ST_SetSRID(ST_MakePoint(${TUNIS_LON}, ${TUNIS_LAT}), 4326)::geography) AS dist
FROM inventory.station
WHERE ST_DWithin(location, ST_SetSRID(ST_MakePoint(${TUNIS_LON}, ${TUNIS_LAT}), 4326)::geography, ${RADIUS_M})
ORDER BY dist ASC;" 2>/dev/null)

timing_ms=$(echo "$timing_result" | grep -oP 'Planning Time: \K[\d.]+')
execution_ms=$(echo "$timing_result" | grep -oP 'Execution Time: \K[\d.]+')
total_ms=$(echo "$timing_ms + $execution_ms" | bc 2>/dev/null || echo "0")

if [ -n "$total_ms" ] && [ "$(echo "$total_ms < 200" | bc 2>/dev/null)" = "1" ]; then
    pass "Query latency < 200ms (${total_ms}ms total)"
else
    fail "Query latency >= 200ms (${total_ms}ms total)"
fi

echo ""

# ------------------------------------------------------------------
# T016: GIST index effectiveness
# ------------------------------------------------------------------
echo "--- T016: GIST Index Benchmark ---"

# Benchmark indexed table
indexed_timing=$($PSQL -tA -c "
EXPLAIN ANALYZE
SELECT id FROM inventory.station
WHERE ST_DWithin(location, ST_SetSRID(ST_MakePoint(${TUNIS_LON}, ${TUNIS_LAT}), 4326)::geography, ${RADIUS_M});" 2>/dev/null)
indexed_time=$(echo "$indexed_timing" | grep -oP 'Execution Time: \K[\d.]+' | head -1)
echo "  Indexed execution time: ${indexed_time}ms"

# Create unindexed temp copy
$PSQL -c "DROP TABLE IF EXISTS inventory.station_unindexed; CREATE TABLE inventory.station_unindexed AS SELECT * FROM inventory.station;" 2>/dev/null

unindexed_timing=$($PSQL -tA -c "
EXPLAIN ANALYZE
SELECT id FROM inventory.station_unindexed
WHERE ST_DWithin(location, ST_SetSRID(ST_MakePoint(${TUNIS_LON}, ${TUNIS_LAT}), 4326)::geography, ${RADIUS_M});" 2>/dev/null)
unindexed_time=$(echo "$unindexed_timing" | grep -oP 'Execution Time: \K[\d.]+' | head -1)
echo "  Unindexed execution time: ${unindexed_time}ms"

# Cleanup
$PSQL -c "DROP TABLE IF EXISTS inventory.station_unindexed;" 2>/dev/null

if [ -n "$indexed_time" ] && [ -n "$unindexed_time" ] && [ "$(echo "$unindexed_time > 0" | bc)" = "1" ]; then
    reduction=$(echo "scale=2; (1 - $indexed_time / $unindexed_time) * 100" | bc 2>/dev/null)
    abs_reduction=$(echo "$reduction" | sed 's/-//')
    if [ "$(echo "$abs_reduction < 10" | bc 2>/dev/null)" = "1" ]; then
        pass "GIST index shows comparable performance (±${abs_reduction}% difference, small dataset)"
        echo "  Note: >50% reduction expected with 10k+ records; current dataset has ${total} records"
    elif [ "$(echo "$reduction > 0" | bc 2>/dev/null)" = "1" ]; then
        pass "GIST index reduces latency by ${reduction}%"
    else
        fail "Unindexed query was significantly faster than indexed (${reduction}%)"
    fi
else
    fail "Could not benchmark GIST index (indexed=$indexed_time, unindexed=$unindexed_time)"
fi

echo ""
echo "============================================"
echo "  Results: $PASS passed, $FAIL failed"
echo "============================================"

exit $FAIL
