# Quickstart: Driver Service

**Prerequisites**: Rust 1.96+, Docker, `platform_db` running (PostGIS)

---

## 1. Start Infrastructure

```bash
# From repo root
docker compose -f infra/compose/docker-compose.local.yml up -d platform-db
```

## 2. Run the Service

```bash
cd source/services

# Set DB connection (or rely on defaults: localhost:5432, postgres/postgres, platform_db)
export DB_HOST=localhost
export DB_PORT=5432
export DB_USER=postgres
export DB_PASSWORD=postgres
export DB_NAME=platform_db

# Run driver service
cargo run -p driver-service
```

The service starts on `http://localhost:8080`.

## 3. Test Endpoints

```bash
# Health check
curl http://localhost:8080/api/v1/health

# List stations
curl http://localhost:8080/api/v1/stations

# Nearby search (Tunis center, 50km radius)
curl "http://localhost:8080/api/v1/stations/nearby?lat=36.8065&lng=10.1815&radius_m=50000"

# Station detail
curl http://localhost:8080/api/v1/stations/STA-abc123def456
```

## 4. Run Tests

```bash
cargo test -p driver-service
```

## Example Session

```bash
# Terminal 1: Start service
cd source/services && cargo run -p driver-service

# Terminal 2: Test endpoints
curl -s http://localhost:8080/api/v1/health | jq .
# {"data":{"status":"ok","database":"connected"},"error":null,"meta":null}

curl -s http://localhost:8080/api/v1/stations | jq '.meta.count'
# 10

curl -s "http://localhost:8080/api/v1/stations/nearby?lat=36.8065&lng=10.1815&radius_m=5000" | jq '.data[].name'
# "Station Alpha"
# "Station Beta"
```
