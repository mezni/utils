# Sprint 02 — Quickstart

**Prerequisites**: Rust 1.88+, PostgreSQL 16+, SQLx CLI

---

## 1. Database Setup

Ensure PostgreSQL is running with the `gis` schema and `find_nearby_stations` function from Sprint 01:

```bash
psql -h localhost -U bornemap -d bornemap \
  -f migrations/platform_db/gis/001_create_schema.sql \
  -f migrations/platform_db/gis/002_create_staging_table.sql \
  -f migrations/platform_db/gis/003_create_curated_table.sql \
  -f migrations/platform_db/gis/004_find_nearby_stations.sql
```

## 2. Environment

```bash
export DATABASE_URL="postgres://bornemap:bornemap@localhost:5432/bornemap"
```

## 3. Build & Run

```bash
cd source/services/driver-service
cargo run
```

## 4. Test Endpoints

```bash
# Health check
curl http://localhost:3001/api/v1/health

# Nearby stations (Tunis center, 5km radius)
curl "http://localhost:3001/api/v1/stations/nearby?lat=36.8065&lon=10.1815&radius=5000&limit=10"

# Error: missing lat
curl "http://localhost:3001/api/v1/stations/nearby?lon=10.1815"

# Error: invalid lat
curl "http://localhost:3001/api/v1/stations/nearby?lat=999&lon=10.1815"
```

## 5. Run Tests

```bash
cargo test
```
