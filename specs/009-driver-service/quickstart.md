# Quickstart: Driver Service

## Prerequisites

- Rust toolchain (1.85+)
- PostgreSQL 17 with PostGIS 3
- Database with Sprint 2.2 migrations applied and seeds loaded
- Docker (optional, for containerized deployment)

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_URL` | Yes | — | PostgreSQL connection string |
| `HOST` | No | `0.0.0.0` | Bind address |
| `PORT` | No | `8080` | Bind port |
| `RUST_LOG` | No | `info` | Log level |

## Run Locally

```bash
# From repository root
DATABASE_URL="postgres://postgres:borne_map@localhost:5432/borne_map" \
  cargo run --package driver-service
```

## Run with Docker

```bash
# Build
docker build -t borne-driver-service -f source/apps/driver-service/Dockerfile .

# Run
docker run -p 8080:8080 \
  -e DATABASE_URL="postgres://postgres:borne_map@host.docker.internal:5432/borne_map" \
  borne-driver-service
```

## Verify

```bash
# Health check
curl http://localhost:8080/api/health

# Nearby stations (Tunis centre, 10km radius)
curl "http://localhost:8080/api/stations/nearby?lat=36.8008&lng=10.1815&radius=10000"

# Map markers (Tunis bbox)
curl "http://localhost:8080/api/stations/markers?south=36.7&west=10.0&north=36.9&east=10.3"

# Search
curl "http://localhost:8080/api/stations/search?q=Tunis"

# Station detail
curl "http://localhost:8080/api/stations/STN001"

# Reviews stub
curl "http://localhost:8080/api/stations/STN001/reviews"
```

## Run Tests

```bash
# Requires test database
DATABASE_URL="postgres://postgres:borne_map@localhost:5432/borne_map_test" \
  cargo test --package driver-service
```
