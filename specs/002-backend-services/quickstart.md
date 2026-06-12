# Quickstart: Backend Services

## Prerequisites

- Phase 1 infra running (see `specs/001-infra-database-setup/quickstart.md`)
- Rust 1.80+ (`rustup install 1.80`)
- Docker Engine 24+

## Setup

```bash
# 1. Phase 1 infra must be running (databases + migrations)
bash scripts/dev.sh

# 2. Build all services
cd source/services
cargo build --workspace

# 3. Set up environment (in source/services/)
cp .env.example .env   # Review and adjust paths
```

## Running Locally

Each service runs as a separate process. Start both in separate terminals:

**Terminal 1 — Driver-Service:**
```bash
cd source/services
DATABASE_URL=postgresql://borneadmin:borne_dev_2026@localhost:5432/platform_db \
cargo run -p driver-service
```

**Terminal 2 — Admin-Service:**
```bash
cd source/services
PLATFORM_DB_URL=postgresql://borneadmin:borne_dev_2026@localhost:5432/platform_db \
ANALYTICS_DB_URL=postgresql://borneadmin:borne_dev_2026@localhost:5433/analytics_db \
cargo run -p admin-service
```

## Testing API Endpoints

### Driver Service (:8080)

```bash
# Health check
curl http://localhost:8080/health

# List stations
curl "http://localhost:8080/api/v1/stations?page=1&per_page=10"

# Nearby search (Tunis city center, 10km)
curl "http://localhost:8080/api/v1/stations/nearby?lat=36.8&lng=10.18&radius=10"

# Station detail
curl http://localhost:8080/api/v1/stations/STA-tunis-centre

# Not found
curl http://localhost:8080/api/v1/stations/STA-nonexistent
```

### Admin Service (:8081)

```bash
# Health check
curl http://localhost:8081/health

# Create station
curl -X POST http://localhost:8081/api/v1/stations \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Test Station",
    "address": "123 Avenue, Tunis",
    "lat": 36.8,
    "lng": 10.18,
    "partner_id": "PRT-totalenergies-tn",
    "chargers": [
      { "type": "CCS2", "power_kw": 150, "price_per_kwh": 0.45 }
    ]
  }'

# Update station
curl -X PUT http://localhost:8081/api/v1/stations/STA-tunis-centre \
  -H "Content-Type: application/json" \
  -d '{"status": "maintenance"}'

# Soft-delete station
curl -X DELETE http://localhost:8081/api/v1/stations/STA-tunis-lac

# Ingest event
curl -X POST http://localhost:8081/api/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "station_viewed",
    "session_id": "sess-test-1",
    "occurred_at": "2026-06-11T12:00:00Z",
    "payload": { "station_id": "STA-tunis-centre" }
  }'

# Ingest batch (up to 100 events)
curl -X POST http://localhost:8081/api/v1/events/batch \
  -H "Content-Type: application/json" \
  -d '{
    "events": [
      { "event_type": "search_performed", "session_id": "sess-test-1", "occurred_at": "2026-06-11T12:00:00Z" }
    ]
  }'
```

## Running Tests

```bash
# From source/services/

# Unit tests (all crates)
cargo test --workspace

# Contract tests (in-process)
cargo test -p driver-service --test contract_*
cargo test -p admin-service --test contract_*

# E2E tests (requires running services, opt-in)
cargo test -p driver-service --test e2e_* -- --ignored
cargo test -p admin-service --test e2e_* -- --ignored

# With coverage
cargo install cargo-tarpaulin
cargo tarpaulin --workspace --out Html
```

## Docker

```bash
# Build and start all services (DBs + Rust services)
docker compose -f infra/docker-compose.yml up -d

# View logs
docker compose -f infra/docker-compose.yml logs -f driver-service admin-service

# Rebuild a single service
docker compose -f infra/docker-compose.yml build driver-service

# Stop everything
docker compose -f infra/docker-compose.yml down
```

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `connection refused` on :5432 | platform-db not running | `docker compose up -d platform-db` |
| `connection refused` on :5433 | analytics-db not running | Same as above |
| `relation "inventory.station" does not exist` | Migrations not run | `bash scripts/init-dbs.sh` |
| `cargo build` fails with sqlx error | Missing sqlx-data.json | Run `cargo sqlx prepare` locally, or set `SQLX_OFFLINE=true` |
| `Permission denied` on port 8080 | Running as non-root | Use `sudo` or port >1024 (defaults already use 8080/8081) |
