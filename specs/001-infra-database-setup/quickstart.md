# Quickstart: Infrastructure & Database Setup

## Prerequisites

- Docker Engine 24+ with Compose plugin
- Git
- psql (PostgreSQL client) for optional manual verification

## Setup

```bash
# 1. Start the database instances
cd /home/dali/WORK/BorneMap
docker compose -f infra/docker-compose.yml up -d

# 2. Wait for health checks to pass
docker compose -f infra/docker-compose.yml ps

# 3. Run platform_db migrations
psql "$PLATFORM_DB_URL" -f infra/migrations/001-platform-db-init.sql
psql "$PLATFORM_DB_URL" -f infra/migrations/002-inventory-schema.sql
psql "$PLATFORM_DB_URL" -f infra/migrations/003-gis-schema.sql

# 4. Run analytics_db migration
psql "$ANALYTICS_DB_URL" -f infra/migrations/004-analytics-db-init.sql

# 5. Load seed data
psql "$PLATFORM_DB_URL" -f infra/migrations/005-seed-data.sql
```

## Verification

```bash
# Verify platform_db schema
psql "$PLATFORM_DB_URL" -c "\dt inventory.*"

# Verify analytics_db schema
psql "$ANALYTICS_DB_URL" -c "\d raw_events"

# Verify seed data
psql "$PLATFORM_DB_URL" -c "SELECT id, name, status FROM inventory.station;"

# Verify spatial index
psql "$PLATFORM_DB_URL" -c "\di inventory.idx_station_location_gist"
```

## Quick Test

```bash
# Nearby search (Tunis city center, 5km radius)
psql "$PLATFORM_DB_URL" <<SQL
SELECT id, name, lat, lng, status,
       ST_Distance(location, ST_SetSRID(ST_Point(10.1815, 36.8065), 4326)) as dist_m
FROM inventory.station
WHERE deleted_at IS NULL
  AND ST_DWithin(location, ST_SetSRID(ST_Point(10.1815, 36.8065), 4326), 5000)
ORDER BY dist_m;
SQL
```

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| Port already in use | Port 5432 or 5433 occupied | Change port in .env and docker-compose.yml |
| Connection refused | DB not ready | Wait for health check: `docker compose ps` |
| Extension not found | PostGIS not enabled | Run `CREATE EXTENSION postgis;` in platform_db |
| Migration duplicate error | Script not idempotent | Add `IF NOT EXISTS` guards |

## Environment

Copy `infra/.env.example` to `.env` and adjust values as needed before starting.
