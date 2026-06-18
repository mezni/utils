# Quickstart: Driver Service & Spatial API

## Prerequisites

- Docker + Docker Compose (with Sprint 1.1 `platform_db` running)
- Rust 1.96+ (stable)

## 1. Environment Setup

```bash
cd source/services/driver-service
cp .env.template .env
# Edit .env with your DATABASE_URL if different
```

### `.env` contents

```env
LISTEN_ADDR=0.0.0.0:3001
DATABASE_URL=postgres://bornemap:bornemap_dev@localhost:5432/platform_db
DB_POOL_MIN=1
DB_POOL_MAX=10
CORS_ORIGINS=*
RUST_LOG=info
```

## 2. Run Locally

```bash
# Ensure the database is running
docker compose -f source/infra/docker-compose.yml up -d platform_db

# Build and run the driver-service
cd source/services/driver-service
cargo run
```

## 3. Test with curl

```bash
# Nearby stations in Tunis
curl "http://localhost:3001/api/v1/nearby?lat=36.8&lng=10.18&radius=10000"

# Health check
curl "http://localhost:3001/health"

# Invalid parameter (should return 400)
curl "http://localhost:3001/api/v1/nearby?lat=999&lng=10.18&radius=10000"

# Radius exceeds max (should return 400)
curl "http://localhost:3001/api/v1/nearby?lat=36.8&lng=10.18&radius=300000"

# No stations in range (should return empty array)
curl "http://localhost:3001/api/v1/nearby?lat=30.0&lng=10.0&radius=1000"
```

## 4. Run with Docker Compose

```bash
# From the repo root
docker compose -f source/infra/docker-compose.yml up -d platform_db driver-service traefik

# Test via Traefik
curl "http://localhost/api/v1/nearby?lat=36.8&lng=10.18&radius=10000"
```

## 5. Run Tests

```bash
cd source/services/driver-service
cargo test
```

## Expected Results

With Sprint 1.1 seed data loaded:
- A 10 km radius around Tunis (36.8, 10.18) returns 4 stations
- A 10 km radius around Sousse (35.828, 10.643) returns 3 stations
- A 10 km radius around Sfax (34.739, 10.755) returns 3 stations
- A 1 km radius around (30.0, 10.0) returns an empty array
