# Driver Service

A Rust service that provides REST API endpoints for the Driver application: health check and stations nearby.

## Overview

- **Framework**: Rust 1.70+ with Actix-web 4.0
- **Database**: PostgreSQL 16 + PostGIS 3.4 (from Sprint 1.1)
- **Testing**: Integration tests with postgres test container
- **Containerization**: Docker with multi-stage Rust build
- **Endpoints**:
  - `GET /api/v1/health` - Service and database health check
  - `GET /api/v1/stations/nearby?lat={lat}&lng={lng}&radius_km={radius}` - Find stations within geographic radius

## Features

- Health check endpoint with database connection verification
- Spatial query using `ST_DWithin` for nearby station search
- Parameter validation (lat: -90 to 90, lng: -180 to 180, radius_km: 0.1 to 100)
- Automatic migration application on startup
- Database connection pooling via `ev-db` shared crate
- JSON API responses
- Error handling with appropriate HTTP status codes

## Prerequisites

- Rust 1.70+
- Docker 24+
- PostgreSQL 16 + PostGIS 3.4 (for development)

## Quick Start

### 1. Build the Docker image

```bash
docker build -t bornemap-driver-service -f Dockerfile .
```

### 2. Start PostgreSQL

```bash
docker compose -f infra/compose/docker-compose.yml up -d postgres
```

### 3. Run the driver service

```bash
docker compose -f infra/compose/docker-compose.yml up -d driver-service
```

### 4. Verify health check

```bash
curl http://localhost:8080/api/v1/health
```

Expected response:
```json
{
  "status": "ok",
  "service": "driver-service",
  "db": "ok"
}
```

### 5. Query stations nearby

```bash
curl "http://localhost:8080/api/v1/stations/nearby?lat=36.8188&lng=10.1657&radius_km=5"
```

## Development

### Run with hot reload

```bash
docker compose -f infra/compose/docker-compose.yml run --rm driver-service sh -c "cargo watch -x run"
```

### Run tests

```bash
cargo test
```

### Run with SQL fixtures

```bash
# Create test database
createdb ev_platform_test

# Apply migrations
sqlx migrate run --database-url postgresql://postgres:postgres@localhost:5432/ev_platform_test

# Run tests
cargo test --database-url postgresql://postgres:postgres@localhost:5432/ev_platform_test
```

## Configuration

| Environment Variable | Required | Default | Description |
|---------------------|----------|---------|-------------|
| `POSTGRES_URL` | Yes | - | PostgreSQL connection URL (format: `postgresql://user:pass@host:port/database`) |
| `LOG_LEVEL` | No | `info` | Logging level (`debug`, `info`, `warn`, `error`) |
| `PORT` | No | `8080` | Service port |

## API Documentation

### Health Check

**Endpoint**: `GET /api/v1/health`

**Response (200 OK)**:
```json
{
  "status": "ok",
  "service": "driver-service",
  "db": "ok"
}
```

**Response (500 Internal Server Error)**:
```json
{
  "error": "Database connection failed"
}
```

**Response (503 Service Unavailable)**:
```json
{
  "error": "Service not running"
}
```

### Stations Nearby

**Endpoint**: `GET /api/v1/stations/nearby`

**Query Parameters**:
- `lat` (required, numeric, -90 to 90) - Latitude of query point
- `lng` (required, numeric, -180 to 180) - Longitude of query point
- `radius_km` (required, numeric, 0.1 to 100) - Radius in kilometers

**Response (200 OK)**:
```json
{
  "stations": [
    {
      "id": "STN-1a2b",
      "name": "Tunis-Belvedere Station",
      "latitude": 36.864702,
      "longitude": 10.158423,
      "distance_km": 1.2
    }
  ]
}
```

**Response (400 Bad Request)**:
```json
{
  "error": "Invalid parameters: latitude must be between -90 and 90"
}
```

**Response (500 Internal Server Error)**:
```json
{
  "error": "Database query failed"
}
```

## Project Structure

```
services/driver-service/
├── src/
│   ├── main.rs              # Entry point, migration application
│   ├── lib.rs               # Library entry point, public API
│   ├── config.rs            # Configuration (Postgres URL)
│   ├── db.rs                # Database connection pool
│   ├── error.rs             # Error types and responses
│   ├── handlers.rs          # Request handlers (health, nearby)
│   ├── models.rs            # Request/response schemas
│   └── routes.rs            # API routes
├── tests/
│   ├── integration_test.rs  # Integration tests
│   └── sql/
│       └── test_stations_nearby.sql  # Test fixtures
├── migrations/              # SQL migrations
├── Dockerfile               # Multi-stage build
├── Dockerfile.dev           # Development Dockerfile
└── Cargo.toml               # Rust project configuration
```

## Testing

### Unit Tests

```bash
cargo test --lib
```

### Integration Tests

```bash
cargo test --test integration_test
```

### Integration Test with SQL Fixtures

```bash
# Run migrations on test database
sqlx migrate run --database-url postgresql://postgres:postgres@localhost:5432/ev_platform_test

# Run tests
cargo test --test integration_test --database-url postgresql://postgres:postgres@localhost:5432/ev_platform_test
```

## Database Schema

The service reads from the following tables (from Sprint 1.2):

### Inventory Schema

- `partner` - Partner information
- `station` - Station information (id, name, latitude, longitude, address, etc.)
- `charger` - Charger information (id, station_id, connector_type, power_kw, status)

### GIS Schema

- `station_locations` - Spatial location data with GiST index for fast spatial queries

## Performance

- **Health Check**: < 50ms
- **Nearby Query**: < 200ms with 15 stations
- **Spatial Query Optimization**: Uses `ST_DWithin` with GiST index on `gis.station_locations.geom`

## Troubleshooting

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
docker exec -it bornemap-postgres psql -U postgres -d ev_platform -c "\di gis.station_locations_geom_gist"

# Manually test the query
docker exec -it bornemap-postgres psql -U postgres -d ev_platform -c "
SELECT COUNT(*) FROM gis.station_locations;
SELECT COUNT(*) FROM inventory.station;
"
```

## Future Enhancements

- Pagination for large result sets
- Authentication and authorization (Sprint 2.x)
- Caching layer for frequent nearby queries
- Detailed error messages with error codes
- Rate limiting
- Monitoring and metrics (Prometheus metrics)
- OpenAPI documentation (Swagger)
