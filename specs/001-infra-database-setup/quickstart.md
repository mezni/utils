# Quickstart: Infrastructure & Database Setup

## Prerequisites

- Docker Engine 24+ with Compose plugin
- Git

## Setup (Single Command)

```bash
# From the repository root
cp infra/.env.example .env          # Review and adjust if needed
scripts/dev.sh                       # Starts everything automatically
```

`dev.sh` handles: env setup, `docker compose up`, health check polling (both DBs), all 5 migrations (001–005), and schema validation.

## Verification

Using `docker compose exec` (no local psql needed):

```bash
# Verify platform_db schema
docker compose -f infra/docker-compose.yml exec platform-db \
  psql -U borneadmin -d platform_db -c "\dt inventory.*"

# Verify analytics_db schema
docker compose -f infra/docker-compose.yml exec analytics-db \
  psql -U borneadmin -d analytics_db -c "\d raw_events"

# Verify seed data
docker compose -f infra/docker-compose.yml exec platform-db \
  psql -U borneadmin -d platform_db -c "SELECT id, name, status FROM inventory.station;"

# Verify spatial index
docker compose -f infra/docker-compose.yml exec platform-db \
  psql -U borneadmin -d platform_db -c "\di inventory.idx_station_location_gist"
```

## Quick Test

```bash
# Nearby search (Tunis city center, 50km radius shows all stations)
docker compose -f infra/docker-compose.yml exec platform-db \
  psql -U borneadmin -d platform_db -c "
SELECT id, name, lat, lng, status,
       ST_Distance(location, ST_SetSRID(ST_Point(10.1815, 36.8065), 4326)) as dist_m
FROM inventory.station
WHERE deleted_at IS NULL
  AND ST_DWithin(location, ST_SetSRID(ST_Point(10.1815, 36.8065), 4326), 50000)
ORDER BY dist_m;
"
```

## Manual Setup (without dev.sh)

```bash
# Start containers
docker compose -f infra/docker-compose.yml up -d

# Run all migrations
bash scripts/init-dbs.sh
```

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| Port already in use | Port 5432 or 5433 occupied | Change port in .env and docker-compose.yml |
| Connection refused | DB not ready | Wait for health check: `docker compose ps` |
| Extension not found | PostGIS not enabled | Run `CREATE EXTENSION postgis;` in platform_db |
| Migration duplicate error | Script not idempotent | Add `IF NOT EXISTS` guards |

## Environment

Copy `infra/.env.example` to `.env` and review before starting. The defaults use `borne_dev_2026` for all database passwords.
