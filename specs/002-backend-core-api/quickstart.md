# Quickstart: MVP-1 Sprint 1 — Backend Core API

## Prerequisites

- Rust toolchain (rustc 1.75+, cargo)
- Docker + Docker Compose (for platform_db)
- Git

## Setup

### 1. Start Infrastructure

```bash
docker compose -f infra/docker-compose.yml up -d
```

This starts `platform_db` (PostgreSQL 16 + PostGIS). Wait for the health check to pass:

```bash
docker compose -f infra/docker-compose.yml ps
```

### 2. Configure Environment

```bash
cp infra/.env.example infra/.env
# Edit infra/.env if needed (defaults work for local dev)
```

### 3. Copy Env for Driver Service

```bash
cp infra/.env source/services/driver-service/.env
```

Or set environment variables directly:

```bash
export DATABASE_URL="postgres://bornemap:bornemap_dev@localhost:5432/platform_db"
export RUST_LOG="driver_service=debug,sea_orm=warn"
```

### 4. Run Migrations (if not auto-applied)

```bash
# Migrations are applied by infrastructure setup scripts,
# or run manually against the running platform_db:
docker compose -f infra/docker-compose.yml exec platform_db \
  psql -U bornemap -d platform_db -f /migrations/001_extensions.sql
docker compose -f infra/docker-compose.yml exec platform_db \
  psql -U bornemap -d platform_db -f /migrations/002_schema_inventory.sql
docker compose -f infra/docker-compose.yml exec platform_db \
  psql -U bornemap -d platform_db -f /migrations/003_seed_stations.sql
```

### 5. Run the Service

```bash
cd source/services/driver-service
cargo run
```

The service starts on `http://localhost:3000`.

## Verify

```bash
# Health check
curl http://localhost:3000/api/v1/health

# List all stations
curl http://localhost:3000/api/v1/stations

# Get station by ID
curl http://localhost:3000/api/v1/stations/STA-00001

# Nearby search (near Tunis center)
curl "http://localhost:3000/api/v1/stations/nearby?lat=36.8&lng=10.2&radius=50000"

# Swagger UI
open http://localhost:3000/api/v1/docs
```

## Run Tests

```bash
cd source/services/driver-service

# All tests
cargo test

# Unit tests only
cargo test --lib

# Integration tests only
cargo test --test integration

# With logs
RUST_LOG=debug cargo test -- --nocapture
```

## Docker Build

```bash
cd source/services/driver-service
docker build -t bornemap/driver-service:latest .
```

## Project Layout

```
source/services/driver-service/
├── Cargo.toml
├── src/
│   ├── main.rs           # Entry point, server setup
│   ├── config.rs         # Environment variable config
│   ├── error.rs          # Typed domain errors
│   └── lib/
│       ├── mod.rs
│       ├── handlers/     # Axum HTTP handlers
│       ├── models/       # Domain types (Station)
│       ├── services/     # Business logic
│       └── repositories/ # PostGIS data access
└── tests/
    ├── fixtures/         # SQL fixture files
    │   └── stations.sql  # Seed data for integration tests
    └── integration/
        ├── mod.rs
        ├── health_test.rs
        ├── stations_test.rs
        └── nearby_test.rs
```
