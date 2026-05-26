# Quickstart: Spatial Discovery — Nearby API & SLO Validation

## Prerequisites

- Phase 1 backend running with seed data loaded
- Docker Compose stack with PostGIS
- `oha` installed: `cargo install oha`

## 1. Verify the Nearby Endpoint

```bash
# Default radius (20km), no test stations
curl "http://localhost:8080/api/v1/stations/nearby?longitude=10.1815&latitude=36.8065"
```

Expected: JSON array of stations ordered by distance, each with
`distance_meters` and `available_chargers_count`. No `is_test = true` stations.

```bash
# Custom radius (50km), with test stations
curl "http://localhost:8080/api/v1/stations/nearby?longitude=10.1815&latitude=36.8065&radius_meters=50000&include_test=true"
```

Expected: More stations including test records.

## 2. Verify Station Detail & Chargers

```bash
# Replace STN-xxx with a known station ID from the nearby response
STATION_ID="STN-seed00000001"

# Station detail
curl "http://localhost:8080/api/v1/stations/$STATION_ID"

# Chargers for station
curl "http://localhost:8080/api/v1/stations/$STATION_ID/chargers"
```

## 3. Run SLO Benchmark

```bash
# 1000 requests, concurrency 10
oha -n 1000 -c 10 \
  "http://localhost:8080/api/v1/stations/nearby?longitude=10.1815&latitude=36.8065"
```

Look for p95 in the output. PASS: p95 ≤ 200ms.

## 4. Analyze Query Plan (if SLO fails)

```bash
# Connect to PostGIS container
docker compose exec postgres psql -U bornemap_admin -d bornemap_dev

# Run EXPLAIN ANALYZE
EXPLAIN ANALYZE
SELECT
    s.id as station_id,
    s.name as station_name,
    s.address,
    s.city,
    ST_X(s.coordinates::geometry) as longitude,
    ST_Y(s.coordinates::geometry) as latitude,
    ST_Distance(s.coordinates, ST_MakePoint(10.1815, 36.8065)::geography) as distance_meters,
    COUNT(c.id) FILTER (WHERE c.status = 'available') as available_chargers_count,
    s.is_test
FROM stations s
LEFT JOIN chargers c ON c.station_id = s.id
WHERE s.deleted_at IS NULL
  AND ST_DWithin(s.coordinates, ST_MakePoint(10.1815, 36.8065)::geography, 20000)
  AND (false = TRUE OR s.is_test = FALSE)
GROUP BY s.id
ORDER BY distance_meters ASC
LIMIT 50;
```

Verify the query uses `Index Scan using idx_stations_coordinates` (GIST index),
not a sequential scan.

## 5. Query Edge Cases

```bash
# Invalid coordinates
curl "http://localhost:8080/api/v1/stations/nearby?longitude=200&latitude=36.8065"
# Expected: 422 Unprocessable Entity

# Negative radius
curl "http://localhost:8080/api/v1/stations/nearby?longitude=10.1815&latitude=36.8065&radius_meters=-1"
# Expected: 422 Unprocessable Entity

# Middle of Sahara (no stations)
curl "http://localhost:8080/api/v1/stations/nearby?longitude=9.5&latitude=26.5"
# Expected: empty array []

# Soft-deleted station detail
curl "http://localhost:8080/api/v1/stations/STN-doesnotexist"
# Expected: 404 Not Found
```
