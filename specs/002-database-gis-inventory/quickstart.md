# Quickstart: Database — GIS and Inventory Schemas

## Prerequisites

- PostgreSQL 16 + PostGIS 3.4 (via Docker Compose from Sprint 1.1)
- psql (PostgreSQL client)
- Docker 24+

## Setup

```bash
# Start PostgreSQL
docker compose -f infra/compose/docker-compose.yml up -d postgres

# Run all migrations
DATABASE_URL=postgres://postgres:postgres@localhost:5432/ev_platform \
  bash db/migrations/migrate.sh

# Load seed data (optional, for development)
psql "$DATABASE_URL" -f db/seeds/dev_partners.sql
psql "$DATABASE_URL" -f db/seeds/dev_stations.sql
psql "$DATABASE_URL" -f db/seeds/dev_chargers.sql
```

## Verify

```bash
# Check schemas exist
psql "$DATABASE_URL" -c "\dn"

# Check inventory tables
psql "$DATABASE_URL" -c "\dt inventory.*"

# Check GIS tables
psql "$DATABASE_URL" -c "\dt gis.*"

# Check spatial query returns results
psql "$DATABASE_URL" -c "
  SELECT COUNT(*) FROM gis.station_locations
  WHERE ST_DWithin(geom, ST_SetSRID(ST_MakePoint(10.0, 36.5), 4326), 50000);
"

# Verify idempotency — run migrations again
DATABASE_URL=postgres://postgres:postgres@localhost:5432/ev_platform \
  bash db/migrations/migrate.sh
```

## Migration Runner

```bash
# Usage
DATABASE_URL=postgres://user:pass@host:5432/dbname bash db/migrations/migrate.sh

# Dry run / validate SQL syntax (no DB connection needed)
psql -f db/migrations/0001_extensions.sql --echo-errors
```
