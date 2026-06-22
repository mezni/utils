# Quickstart Guide: GIS Engine Foundation

**Feature**: 003 - GIS Engine Foundation
**Date**: 2026-06-22
**Version**: 1.0.0

## Prerequisites

- **PostgreSQL 16+ with PostGIS extension**
- **Redis 7+**
- **Rust 1.75+**
- **OSM data source** (overpass-api.de)

## Quick Start (5-Minute Setup)

### Step 1: Start Dependencies

```bash
# PostgreSQL with PostGIS
docker run -d \
  --name bornemap_postgres \
  -e POSTGRES_DB=borne_map \
  -e POSTGRES_USER=borne_map_admin \
  -e POSTGRES_PASSWORD=borne_map_password \
  -p 5432:5432 \
  postgis/postgis:16-alpine

# Redis
docker run -d \
  --name bornemap_redis \
  -p 6379:6379 \
  redis:7-alpine
```

### Step 2: Set Up Database

```bash
# Run database migrations
./infrastructure/scripts/migrate.sh

# Create GIS schema (driver-service only)
psql postgresql://borne_map_admin:borne_map_password@localhost:5432/borne_map \
  -c "CREATE SCHEMA gis;"

# Grant write access to driver-service
psql postgresql://borne_map_admin:borne_map_password@localhost:5432/borne_map \
  -c "GRANT ALL PRIVILEGES ON SCHEMA gis TO borne_map_driver;"
```

### Step 3: Start Driver Service

```bash
# Set environment variables
export APP_DATABASE_URL="postgresql://borne_map_admin:borne_map_password@localhost:5432/borne_map"
export APP_ANALYTICS_DATABASE_URL="postgresql://borne_map_analytics:borne_map_password@localhost:5432/analytics_db"
export APP_REDIS_URL="redis://localhost:6379"
export APP_SERVER_PORT=3001
export APP_JWT_ISSUER="http://localhost:8080/realms/bornemap"
export APP_JWT_AUDIENCE="driver-service-sa"

# Start the service
cargo run --bin driver-service
```

### Step 4: Test Basic Query

```bash
# Test nearby query (requires JWT token from Keycloak)
JWT_TOKEN="Bearer <YOUR_JWT_TOKEN>"

curl -H "Authorization: $JWT_TOKEN" \
  "http://localhost:3001/api/v1/driver/nearby?lat=40.7829&lon=-73.9654&radius=1000"

# Expected output: JSON array of stations within 1km
```

## Common Use Cases

### Use Case 1: Find Nearby Chargers

**Goal**: Driver wants to find charging stations within 10km of current location.

**Steps**:
```bash
# 1. Get JWT token from Keycloak
# Login as driver
LOGIN_RESPONSE=$(curl -s -X POST \
  "http://localhost:8080/realms/bornemap/protocol/openid-connect/token" \
  -d "username=driver@borne.map" \
  -d "password=driver123" \
  -d "grant_type=password" \
  -d "client_id=mobile-driver")

# 2. Extract access token
JWT_TOKEN=$(echo $LOGIN_RESPONSE | jq -r '.access_token')

# 3. Query nearby stations
curl -H "Authorization: Bearer $JWT_TOKEN" \
  "http://localhost:3001/api/v1/driver/nearby?lat=40.7829&lon=-73.9654&radius=10000&limit=10"
```

**Expected Result**:
- Returns up to 10 stations within 10km
- Results ordered by distance from query point
- Each station includes: id, name, latitude, longitude, distance, power, connector_types

### Use Case 2: View Station Details

**Goal**: Driver taps a marker and wants to see station details.

**Steps**:
```bash
# Get JWT token
JWT_TOKEN=$(curl -s -X POST \
  "http://localhost:8080/realms/bornemap/protocol/openid-connect/token" \
  -d "username=driver@borne.map" \
  -d "password=driver123" \
  -d "grant_type=password" \
  -d "client_id=mobile-driver" | jq -r '.access_token')

# Query station details
STATION_ID="STA-abc123456789"

curl -H "Authorization: Bearer $JWT_TOKEN" \
  "http://localhost:3001/api/v1/driver/stations/$STATION_ID"
```

**Expected Result**:
- Returns full station details
- Includes: id, name, latitude, longitude, operator, address, power, connector_types, availability

### Use Case 3: Trigger OSM Ingestion

**Goal**: Admin triggers daily OSM data ingestion.

**Steps**:
```bash
# Get admin JWT token
ADMIN_TOKEN=$(curl -s -X POST \
  "http://localhost:8080/realms/bornemap/protocol/openid-connect/token" \
  -d "username=admin@borne.map" \
  -d "password=admin123" \
  -d "grant_type=password" \
  -d "client_id=admin-dashboard" | jq -r '.access_token')

# Trigger ingestion
curl -X POST \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  "http://localhost:3001/api/v1/gis/ingest"
```

**Expected Result**:
- Returns 202 Accepted with job_id
- Ingestion runs in background
- Check status: `GET /api/v1/gis/ingest/status/{job_id}`

**Expected Result**:
- Returns job status (pending, processing, completed, failed)
- When complete: shows rows_processed, rows_success, rows_failed, duration

### Use Case 4: Map Rendering

**Goal**: Mobile app displays map with charging station markers.

**Steps**:
```bash
# 1. Get JWT token
JWT_TOKEN=$(curl -s -X POST \
  "http://localhost:8080/realms/bornemap/protocol/openid-connect/token" \
  -d "username=driver@borne.map" \
  -d "password=driver123" \
  -d "grant_type=password" \
  -d "client_id=mobile-driver" | jq -r '.access_token')

# 2. Fetch map data (list of stations within viewport)
VIEWPORT_LAT=40.78
VIEWPORT_LON=-73.96
VIEWPORT_ZOOM=12

curl -H "Authorization: Bearer $JWT_TOKEN" \
  "http://localhost:3001/api/v1/driver/stations?lat=$VIEWPORT_LAT&lon=$VIEWPORT_LON&radius=20000"

# 3. App displays stations on map with clustering
```

**Expected Result**:
- JSON array of stations
- Each station includes: id, name, latitude, longitude
- App groups nearby markers (clustering)
- User taps marker to see details

## Configuration

### Environment Variables

**Required**:
```bash
export APP_DATABASE_URL="postgresql://<user>:<password>@localhost:5432/borne_map"
export APP_ANALYTICS_DATABASE_URL="postgresql://<user>:<password>@localhost:5432/analytics_db"
export APP_REDIS_URL="redis://localhost:6379"
export APP_SERVER_PORT=3001
export APP_JWT_ISSUER="http://localhost:8080/realms/bornemap"
export APP_JWT_AUDIENCE="driver-service-sa"
```

**Optional**:
```bash
export APP_JWT_ISSUER="http://localhost:8080/realms/bornemap"
export APP_JWT_AUDIENCE="driver-service-sa"
export APP_REDIS_CACHE_TTL=300  # Cache TTL in seconds (default: 300)
export APP_REDIS_MAX_SIZE=10485760  # Max cache size in bytes (default: 10MB)
export APP_SPATIAL_CACHE_PREFIX="geo:"  # Redis key prefix (default: "geo:")
export APP_LOG_LEVEL="info"  # log level (default: "info")
export APP_MAX_RADIUS=100000  # Maximum radius in meters (default: 100000)
export APP_MIN_RADIUS=100  # Minimum radius in meters (default: 100)
```

### Database Configuration

**Database Roles**:
```sql
-- Driver service can write to gis schema
GRANT ALL PRIVILEGES ON SCHEMA gis TO borne_map_driver;

-- Analytics can read from analytics_db (write-only for telemetry)
GRANT ALL PRIVILEGES ON SCHEMA telemetry TO borne_map_analytics_writer;
GRANT USAGE ON SCHEMA telemetry TO borne_map_analytics_reader;

-- Admin can read from analytics_db (no write access)
GRANT SELECT ON ALL TABLES IN SCHEMA telemetry TO borne_map_analytics_reader;
```

### Redis Configuration

**Redis Keys**:
- Spatial cache: `geo:radius:{lat}:{lon}:{radius}`
- Example: `geo:radius:40.7829:-73.9654:10000`

**Redis TTL**: 5 minutes (configurable)

## Testing

### Unit Tests

```bash
# Run unit tests
cargo test --package driver-service --lib

# Run integration tests
cargo test --package driver-service --test integration

# Run with coverage
cargo tarpaulin --package driver-service
```

### Integration Tests

```bash
# Test nearby query
cargo test --package driver-service --test integration -- test_nearby_query

# Test station details
cargo test --package driver-service --test integration -- test_station_detail

# Test ingestion
cargo test --package driver-service --test integration -- test_osm_ingestion
```

### Manual Testing

```bash
# Start services
cd /home/dali/WORK/BorneMap

# Test health endpoint
curl http://localhost:3001/health

# Test nearby query (requires JWT)
JWT_TOKEN=$(curl -s -X POST \
  "http://localhost:8080/realms/bornemap/protocol/openid-connect/token" \
  -d "username=driver@borne.map" \
  -d "password=driver123" \
  -d "grant_type=password" \
  -d "client_id=mobile-driver" | jq -r '.access_token')

curl -H "Authorization: Bearer $JWT_TOKEN" \
  "http://localhost:3001/api/v1/driver/nearby?lat=40.7829&lon=-73.9654&radius=1000"

# Test station details
curl -H "Authorization: Bearer $JWT_TOKEN" \
  "http://localhost:3001/api/v1/driver/stations/STA-abc123456789"

# Test ingestion trigger
ADMIN_TOKEN=$(curl -s -X POST \
  "http://localhost:8080/realms/bornemap/protocol/openid-connect/token" \
  -d "username=admin@borne.map" \
  -d "password=admin123" \
  -d "grant_type=password" \
  -d "client_id=admin-dashboard" | jq -r '.access_token')

curl -X POST \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  "http://localhost:3001/api/v1/gis/ingest"
```

## Troubleshooting

### Issue: "PostGIS extension not found"

**Solution**:
```bash
# Rebuild PostgreSQL with PostGIS extension
docker run -d \
  --name bornemap_postgres \
  -e POSTGRES_DB=borne_map \
  -e POSTGRES_USER=borne_map_admin \
  -e POSTGRES_PASSWORD=borne_map_password \
  -p 5432:5432 \
  postgis/postgis:16-alpine
```

### Issue: Redis connection refused

**Solution**:
```bash
# Check Redis status
docker ps | grep redis

# Restart Redis if needed
docker restart bornemap_redis

# Verify Redis is listening
redis-cli ping  # Should return "PONG"
```

### Issue: No stations returned

**Solution**:
```bash
# Check if driver-service is running
curl http://localhost:3001/health

# Check Redis cache status
redis-cli KEYS "geo:*"

# Manually query PostGIS
psql postgresql://borne_map_admin:borne_map_password@localhost:5432/borne_map \
  -c "SELECT COUNT(*) FROM gis.osm_charging_stations;"

# Trigger OSM ingestion (admin-only)
ADMIN_TOKEN=$(curl -s -X POST \
  "http://localhost:8080/realms/bornemap/protocol/openid-connect/token" \
  -d "username=admin@borne.map" \
  -d "password=admin123" \
  -d "grant_type=password" \
  -d "client_id=admin-dashboard" | jq -r '.access_token')

curl -X POST \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  "http://localhost:3001/api/v1/gis/ingest"
```

### Issue: Spatial query timeout

**Solution**:
```bash
# Check if spatial index exists
psql postgresql://borne_map_admin:borne_map_password@localhost:5432/borne_map \
  -c "\d gis.osm_charging_stations"

# Rebuild spatial index if missing
psql postgresql://borne_map_admin:borne_map_password@localhost:5432/borne_map \
  -c "CREATE INDEX idx_stations_geo ON gis.osm_charging_stations USING GiST (geom);"
```

## Next Steps

After completing the quickstart, continue with:

1. **Explore API Endpoints**: Test all spatial query endpoints
2. **Customize Configuration**: Adjust cache TTL, max radius, etc.
3. **Monitor Performance**: Check Redis cache hit rate, query response times
4. **Security**: Set up proper JWT validation, RBAC
5. **Monitoring**: Set up logging, metrics, alerts
6. **Documentation**: Add API documentation (Swagger/OpenAPI)

## Additional Resources

- **PostGIS Documentation**: https://postgis.net/documentation/
- **Redis Documentation**: https://redis.io/documentation/
- **Overpass API**: https://overpass-api.de/DE/api/Overpass
- **GIS Best Practices**: https://developers.google.com/maps/documentation/geojson

## Support

For issues or questions:
1. Check the API contracts: `specs/003-gis-engine/contracts/api-contracts.md`
2. Review the data model: `specs/003-gis-engine/data-model.md`
3. Check the implementation plan: `specs/003-gis-engine/plan.md`
4. Review the constitution: `docs/constitution/constitution.md`

---

**Version**: 1.0.0
**Last Updated**: 2026-06-22
