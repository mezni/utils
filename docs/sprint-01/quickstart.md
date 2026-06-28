# Sprint 01 — Quickstart

## Prerequisites
- Rust 1.90+ (`rustup install 1.90` or later)
- Node.js 20+
- Docker & Docker Compose
- PostgreSQL 15+ with PostGIS (via Docker recommended)

## Setup

```bash
# 1. Start infrastructure
docker compose up -d postgres

# 2. Run database migrations
./scripts/migrate.sh

# 3. Build all Rust services
cargo build --workspace

# 4. Run tests
cargo test --workspace

# 5. Start a service (example: auth-service)
cargo run -p auth-service

# 6. Verify health endpoint
curl http://localhost:3001/health
```

## Frontend

```bash
cd apps/admin-dashboard
npm install
npm run dev
```

## Verify

```bash
# All services respond
curl http://localhost:3001/health  # auth-service
curl http://localhost:3002/health  # admin-service
curl http://localhost:3003/health  # driver-service
```
