# Quickstart: Local Development Setup

## Prerequisites

- Docker & Docker Compose v2.24+
- Rust toolchain (rustup)
- Node.js 20+ (for Expo mobile app)
- pnpm (for frontend monorepo)

## 1. Clone and Enter

```bash
git clone git@github.com:mezni/BorneMap.git
cd BorneMap
```

## 2. Start Infrastructure

```bash
docker compose -f infra/docker-compose.yml up -d
```

This starts:
- platform_db (PostgreSQL 16 + PostGIS) on port 5432
- analytics_db (PostgreSQL 16) on port 5433
- Keycloak on port 8083

## 3. Verify Health

```bash
# Check databases are reachable
psql -h localhost -p 5432 -U postgres -d platform_db -c "SELECT 1;"
psql -h localhost -p 5433 -U postgres -d analytics_db -c "SELECT 1;"

# Verify PostGIS
psql -h localhost -p 5432 -U postgres -d platform_db -c "SELECT PostGIS_Version();"
```

## 4. Run Services (Future Sprints)

```bash
# Driver Service
cd source/services/driver-service && cargo run

# Clickstream Service
cd source/services/clickstream-service && cargo run
```

## 5. Stop Everything

```bash
docker compose -f infra/docker-compose.yml down
```

## Troubleshooting

| Issue | Fix |
|---|---|
| Port conflict on 5432 | Check `lsof -i :5432` and stop conflicting service |
| Docker not installed | Install Docker Desktop or Docker Engine |
| Stale containers | `docker compose -f infra/docker-compose.yml down -v` |
| PostGIS not enabled | Run `CREATE EXTENSION postgis;` on platform_db |
