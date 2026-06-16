# Quickstart — GIS Data & Nearby Discovery

**Feature**: GIS Data & Nearby Discovery — MVP-2 Sprint 2.0
**Last Updated**: 2026-06-16

## Overview

This quickstart guide provides step-by-step instructions for setting up and using the GIS data layer, including database setup, data import, and testing the nearby API.

## Prerequisites

- Docker Compose v2 installed
- Local environment with `.env` file configured
- Access to internet (for OSM API calls)
- Node.js 20+ with pnpm (for driver apps)
- Rust 1.80+ with Cargo (for driver service)

## Quick Setup (5 Minutes)

### 1. Start the Database

```bash
cd /home/dali/WORK/BorneMap
docker compose --profile infra up -d platform-db
```

### 2. Create Inventory Schema

```bash
# Connect to the database
docker compose exec platform-db psql -U bornemap -d bornemap -c "
  CREATE SCHEMA IF NOT EXISTS inventory;
  CREATE SCHEMA IF NOT EXISTS gis;
"

# Enable PostGIS extension
docker compose exec platform-db psql -U bornemap -d bornemap -c "
  CREATE EXTENSION IF NOT EXISTS postgis;
"
```

### 3. Create Tables

```bash
# Create station table
docker compose exec platform-db psql -U bornemap -d bornemap -c "
  CREATE TABLE IF NOT EXISTS inventory.station (
    id VARCHAR(32) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    visibility VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'draft',
    location GEOGRAPHY(POINT, 4326) NOT NULL,
    address TEXT,
    city VARCHAR(100),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ,
    CONSTRAINT station_visibility_check CHECK (visibility IN ('commercial', 'private_home', 'all')),
    CONSTRAINT station_status_check CHECK (status IN ('draft', 'active', 'inactive', 'closed'))
  );
"

# Create charger table
docker compose exec platform-db psql -U bornemap -d bornemap -c "
  CREATE TABLE IF NOT EXISTS inventory.charger (
    id VARCHAR(32) PRIMARY KEY,
    station_id VARCHAR(32) NOT NULL REFERENCES inventory.station(id) ON DELETE CASCADE,
    connector_type VARCHAR(50) NOT NULL,
    connector_count INTEGER NOT NULL DEFAULT 1 CHECK (connector_count >= 1 AND connector_count <= 100),
    power_kw DECIMAL(5,2) NOT NULL CHECK (power_kw >= 0 AND power_kw <= 999.99),
    status VARCHAR(50) NOT NULL DEFAULT 'available',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ,
    CONSTRAINT charger_connector_type_check CHECK (connector_type IN ('type1', 'type2', 'ccs', 'chademo', 'other')),
    CONSTRAINT charger_status_check CHECK (status IN ('available', 'occupied', 'unavailable'))
  );
"

# Create import log table
docker compose exec platform-db psql -U bornemap -d bornemap -c "
  CREATE TABLE IF NOT EXISTS gis.import_log (
    id SERIAL PRIMARY KEY,
    status VARCHAR(50) NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ,
    bbox JSONB NOT NULL,
    stations_imported INTEGER NOT NULL DEFAULT 0,
    stations_updated INTEGER NOT NULL DEFAULT 0,
    stations_failed INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT import_log_status_check CHECK (status IN ('success', 'failed'))
  );
"
```

### 4. Create Spatial Indexes

```bash
docker compose exec platform-db psql -U bornemap -d bornemap -c "
  CREATE INDEX IF NOT EXISTS idx_station_location ON inventory.station USING GIST (location);
  CREATE INDEX IF NOT EXISTS idx_station_status ON inventory.station (status);
  CREATE INDEX IF NOT EXISTS idx_station_visibility ON inventory.station (visibility);
  CREATE INDEX IF NOT EXISTS idx_charger_station ON inventory.charger (station_id);
  CREATE INDEX IF NOT EXISTS idx_charger_status ON inventory.charger (status);
  CREATE INDEX IF NOT EXISTS idx_charger_connector_type ON inventory.charger (connector_type);
  CREATE INDEX IF NOT EXISTS idx_import_log_time ON gis.import_log (start_time DESC);
  CREATE INDEX IF NOT EXISTS idx_import_log_status ON gis.import_log (status);
"
```

### 5. Run Import Process

```bash
# Start import container
docker compose --profile import up osm-importer
```

Expected output:
```
Fetching charging station data for Tunisia region...
Importing 1250 stations...
Updating 340 existing stations...
Completed: 1250 imported, 340 updated, 0 failed
```

### 6. Test Nearby API

```bash
# Get a valid JWT token from auth service
# For now, we'll use a sample token
TOKEN="your_jwt_token_here"

# Test the nearby endpoint
curl -X GET "http://localhost:3001/api/v1/nearby?lat=36.8&lon=10.18&radius_m=5000" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" | jq .
```

Expected response:
```json
{
  "data": {
    "stations": [
      {
        "id": "sta_xxxxx",
        "name": "Station Menzah",
        "location": { "lat": 36.84, "lon": 10.19 },
        "address": "Rue des Jasmins, Menzah",
        "city": "Tunis",
        "distance_m": 1240,
        "visibility": "commercial",
        "status": "active",
        "chargers": [...]
      }
    ],
    "count": 1,
    "radius_m": 5000
  }
}
```

## Development Workflow

### Starting All Services

```bash
# Start databases and services
docker compose --profile infra --profile services up -d

# Start import container (for data refresh)
docker compose --profile import up -d osm-importer
```

### Re-running Import

```bash
# Stop current import
docker compose --profile import down osm-importer

# Re-run import
docker compose --profile import up osm-importer

# Check import log
docker compose exec platform-db psql -U bornemap -d bornemap -c "
  SELECT * FROM gis.import_log ORDER BY start_time DESC LIMIT 5;
"
```

### Verifying Data

```bash
# Check total stations
docker compose exec platform-db psql -U bornemap -d bornemap -c "
  SELECT COUNT(*) FROM inventory.station WHERE deleted_at IS NULL;
"

# Check active stations
docker compose exec platform-db psql -U bornemap -d bornemap -c "
  SELECT COUNT(*) FROM inventory.station WHERE status = 'active' AND deleted_at IS NULL;
"

# Check stations in Tunis
docker compose exec platform-db psql -U bornemap -d bornemap -c "
  SELECT COUNT(*) FROM inventory.station
  WHERE city = 'Tunis' AND status = 'active' AND deleted_at IS NULL;
"

# Check station details
docker compose exec platform-db psql -U bornemap -d bornemap -c "
  SELECT id, name, visibility, status, city
  FROM inventory.station
  WHERE status = 'active' AND deleted_at IS NULL
  LIMIT 5;
"
```

### Running SQL Tests

```bash
# Test gis.nearby() function
docker compose exec platform-db psql -U bornemap -d bornemap -c "
  SELECT * FROM gis.nearby(36.8, 10.18, 5000, 10);
"

# Test ST_DWithin query
docker compose exec platform-db psql -U bornemap -d bornemap -c "
  SELECT id, name, ST_Distance(
    location,
    ST_GeogFromText('SRID=4326;POINT(10.18 36.8)')
  ) AS distance_m
  FROM inventory.station
  WHERE ST_DWithin(
    location,
    ST_GeogFromText('SRID=4326;POINT(10.18 36.8)'),
    5000
  )
  ORDER BY distance_m ASC
  LIMIT 10;
"
```

### Testing Driver Apps

```bash
# Mobile driver app
cd apps/mobile-driver
npx expo start -c

# Web driver app
cd apps/web-driver
npm run dev
```

### Common Tasks

#### Reset Database for Fresh Import

```bash
# Drop and recreate inventory schema
docker compose exec platform-db psql -U bornemap -d bornemap -c "
  DROP SCHEMA inventory CASCADE;
  DROP SCHEMA gis CASCADE;
  CREATE SCHEMA inventory;
  CREATE SCHEMA gis;
"

# Re-run import
docker compose --profile import up osm-importer
```

#### Update Map Tiles

```bash
# Check PostGIS version
docker compose exec platform-db psql -U bornemap -d bornemap -c "
  SELECT PostGIS_Version();
"
```

#### Monitor Database Logs

```bash
# Follow database logs
docker compose -f infra/docker-compose.yml logs -f platform-db
```

## Troubleshooting

### Import Process Fails

**Problem**: Import container exits with error

**Solution**:
1. Check logs: `docker compose logs osm-importer`
2. Verify database connection: `docker compose exec platform-db psql -U bornemap -d bornemap -c "SELECT 1"`
3. Check OSM API connectivity: `curl https://overpass-api.de/api/interpreter`

**Example**:
```bash
# Check logs
docker compose logs osm-importer

# Check database connection
docker compose exec platform-db psql -U bornemap -d bornemap -c "SELECT 1"

# Retry import
docker compose --profile import up osm-importer
```

### Spatial Query Performance Issue

**Problem**: Query takes longer than 5 seconds

**Solution**:
1. Verify GIST index exists: `docker compose exec platform-db psql -U bornemap -d bornemap -c "\d+ inventory.station"`
2. Analyze table: `docker compose exec platform-db psql -U bornemap -d bornemap -c "ANALYZE inventory.station"`
3. Check query plan: `docker compose exec platform-db psql -U bornemap -d bornemap -c "EXPLAIN SELECT * FROM gis.nearby(36.8, 10.18, 5000);"`

### Rate Limiting Error

**Problem**: Getting 429 Too Many Requests

**Solution**:
1. Wait for rate limit to reset (60 seconds)
2. Check current usage in logs
3. Verify rate limiting configuration

### Empty Results

**Problem**: Nearby API returns empty stations array

**Possible Causes**:
1. No stations in queried area
2. All stations are inactive or deleted
3. Invalid coordinates

**Solution**:
```bash
# Check total active stations
docker compose exec platform-db psql -U bornemap -d bornemap -c "
  SELECT COUNT(*) FROM inventory.station
  WHERE status = 'active' AND deleted_at IS NULL;
"

# Check stations near your coordinates
docker compose exec platform-db psql -U bornemap -d bornemap -c "
  SELECT COUNT(*) FROM inventory.station
  WHERE ST_DWithin(
    location,
    ST_GeogFromText('SRID=4326;POINT(10.18 36.8)'),
    5000
  )
  AND status = 'active' AND deleted_at IS NULL;
"
```

## Performance Benchmarks

| Metric | Target | Typical Value |
|--------|--------|---------------|
| Spatial query latency | < 5 seconds | 1-3 seconds |
| Import process time | < 10 minutes | 5-8 minutes |
| API response time | < 1 second | 200-500ms |
| Marker rendering | < 100ms | 50-80ms |

## Next Steps

1. **Review Data Model**: See [data-model.md](./data-model.md) for detailed entity definitions
2. **Review API Contracts**: See [contracts/api.md](./contracts/api.md) for endpoint specifications
3. **Review Full Plan**: See [plan.md](./plan.md) for implementation details

## Additional Resources

- [PostGIS Documentation](https://postgis.net/documentation/)
- [Overpass API Reference](https://overpass-api.de/OverpassAPI.html)
- [React Native Maps](https://github.com/react-native-mapbox-gl/maps)
- [Mapbox GL JS](https://docs.mapbox.com/mapbox-gl-js/api/)
