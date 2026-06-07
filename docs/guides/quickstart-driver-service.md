# Driver Service Quickstart Guide

## Prerequisites

- **Rust 1.70+** - Install via rustup (https://rustup.rs)
- **Docker** 24+ - Docker Desktop or Docker CLI
- **Docker Compose** - Included with Docker Desktop

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/mezni/BorneMap.git
cd BorneMap
```

### 2. Create database URL

```bash
export POSTGRES_URL=postgres://postgres:postgres@localhost:5432/ev_platform
```

### 3. Build the Docker image

```bash
docker build -t bornemap-driver-service -f services/driver-service/Dockerfile services/driver-service
```

### 4. Start PostgreSQL and Driver Service

```bash
docker compose -f infra/compose/docker-compose.yml up -d postgres

# Wait for PostgreSQL to be ready (watch logs):
docker logs -f postgres

# When you see "database system is ready to accept connections", press Ctrl+C and start the driver service:

docker compose -f infra/compose/docker-compose.yml up -d driver-service
```

### 5. Verify service is running

```bash
curl http://localhost:8080/api/v1/health
```

Expected response:
```json
{"status":"ok","service":"driver-service","db":"ok"}
```

## Running Tests

### Run all tests

```bash
cd services/driver-service
cargo test
```

### Run specific test

```bash
cargo test test_stations_nearby
```

### Run with SQL query tests

```bash
cargo test --test integration_test
```

## Development Setup

### Install dev dependencies

```bash
cd services/driver-service
cargo install sqlx-cli
```

### Run with hot reload

```bash
cargo watch -x run
```

### Run tests with SQL fixtures

```bash
# Create test database
createdb bornemap_test

# Run migrations on test database
sqlx migrate run --database-url postgresql://postgres:postgres@localhost:5432/ev_platform_test

# Run tests with test database
cargo test --database-url postgresql://postgres:postgres@localhost:24321/ev_platform_test
```

## Integration Testing

### Test the nearby endpoint

```bash
# Query for stations within 5km of Tunis coordinates
curl "http://localhost:8080/api/v1/stations/nearby?lat=36.8188&lng=10.1657&radius_km=5" | jq
```

Expected response (with seeded data):
```json
{
  "stations": [
    {
      "id": "STN-1a2b",
      "name": "Tunis-Belvedere Station",
      "latitude": 36.864702,
      "longitude": 10.158423,
      "distance_km": 1.2
    },
    {
      "id": "STN-2c3d",
      "name": "Hammamet Station",
      "latitude": 36.846200,
      "longitude": 10.180000,
      "distance_km": 2.5
    }
  ]
}
```

### Test with no stations nearby

```bash
# Query for stations near Antarctica (no stations within 100km)
curl "http://localhost:8080/api/v1/stations/nearby?lat=-90&lng=0&radius_km=100" | jq
```

Expected response:
```json
{
  "stations": []
}
```

### Test invalid parameters

```bash
# Invalid latitude
curl "http://localhost:8080/api/v1/stations/nearby?lat=91&lng=0&radius_km=5"

# Negative radius
curl "http://localhost:8080/api/v1/stations/nearby?lat=36.8188&lng=10.1657&radius_km=-1"
```

Expected response (400 Bad Request):
```json
{
  "error": "Invalid parameters: latitude must be between -90 and 90"
}
```

## Database Setup

### Connect to database

```bash
docker exec -it bornemap-postgres psql -U postgres -d ev_platform
```

### Verify tables exist

```sql
\dt inventory.*
\dt gis.*
```

### Check stations nearby query

```sql
SELECT id, name, latitude, longitude,
       ST_DWithin(
         gis.station_locations.geom,
         ST_SetSRID(ST_MakePoint(10.1657, 36.8188), 4326),
         5000
       ) AS is_within_5km
FROM gis.station_locations, inventory.station
WHERE station_locations.station_id = inventory.station.id;
```

### View seed data

```sql
-- Count partners
SELECT COUNT(*) FROM inventory.partner;

-- Count stations
SELECT COUNT(*) FROM inventory.station;

-- Count chargers
SELECT COUNT(*) FROM inventory.charger;

-- See station details
SELECT id, name, latitude, longitude FROM inventory.station LIMIT 5;
```

## Troubleshooting

### Service won't start

```bash
# Check logs
docker logs bornemap-driver-service

# Check PostgreSQL is running
docker ps | grep postgres

# Check database URL is set
echo $POSTGRES_URL
```

### Health check returns "db": "error"

```bash
# Verify PostgreSQL is accessible
docker exec -it bornemap-postgres psql -U postgres -d ev_platform -c "SELECT 1;"

# Verify POSTGRES_URL includes database name
# Correct: postgresql://user:pass@host:port/database
# Incorrect: postgresql://user:pass@host:port
```

### Nearby endpoint returns empty array when stations exist

```bash
# Check that migrations were applied
docker exec -it bornemap-postgres psql -U postgres -d ev_platform -c "\dt gis.station_locations"

# Check that GiST indexes exist
docker exec -it bornemap-postgres psql -U postgres -d ev_platform -c "\di gis.station_locations_geom"

# Manually test the query
docker exec -it bornemap-postgres psql -U postgres -d ev_platform -c "
SELECT COUNT(*) FROM gis.station_locations;
SELECT COUNT(*) FROM inventory.station;
"
```

### Test with seeded database

```bash
# Ensure migrations are applied
docker exec -it bornemap-postgres psql -U postgres -d ev_platform -f db/seeds/dev_partners.sql
docker exec -it bornemap-postgres psql -U postgres -d ev_platform -f db/seeds/dev_stations.sql
docker exec -it bornemap-postgres psql -U postgres -d ev_platform -f db/seeds/dev_chargers.sql
```

## Performance Testing

### Measure response time

```bash
# Health check
time curl http://localhost:8080/api/v1/health

# Nearby query
time curl "http://localhost:8080/api/v1/stations/nearby?lat=36.8188&lng=10.1657&radius_km=5"
```

### Benchmark with 100 concurrent requests

```bash
# Run Apache Bench
ab -n 100 -c 10 http://localhost:8080/api/v1/health

# Run wrk
wrk -t4 -c100 -d10s http://localhost:8080/api/v1/health
```

### Verify spatial query optimization

```bash
# Check index usage
docker exec -it bornemap-postgres psql -U postgres -d ev_platform -c "
EXPLAIN ANALYZE
SELECT s.id, s.name, s.latitude, s.longitude,
       ST_DWithin(g.geom, ST_SetSRID(ST_MakePoint(10.1657, 36.8188), 4326)
FROM gis.station_locations g, inventory.station s
WHERE g.station_id = s.id
LIMIT 10;
"

# Check index usage
docker exec -it bornemap-postgres psql -U postgres -d ev_platform -c "
SELECT indexrelid::regclass, idx_scan FROM pg_stat_user_indexes WHERE indexrelname LIKE '%station_locations%';
"
```

## Clean Up

### Stop services

```bash
docker compose -f infra/compose/docker-compose.yml down
```

### Remove volumes (complete reset)

```bash
docker compose -f infra/compose/docker-compose.yml down -v
```

## Next Steps

1. **Test all endpoints**: Health check and nearby endpoint
2. **Run integration tests**: `cargo test`
3. **Deploy to test environment**: Build Docker image and deploy
4. **Begin Sprint 1.4**: Admin Service (after Sprint 1.3 validation)

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| POSTGRES_URL | Yes | - | PostgreSQL connection URL (format: `postgresql://user:pass@host:port/database`) |
| LOG_LEVEL | No | info | Logging level (debug, info, warn, error) |
| PORT | No | 8080 | Service port |