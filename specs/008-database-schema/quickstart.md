# Quickstart: Database Schema

## Prerequisites

- PostgreSQL 17 with PostGIS 3 extension
- psql CLI or any PostgreSQL client

## Apply Migrations

From the repository root, apply migrations in order:

```bash
# Create the database (one-time)
createdb borne_map

# Apply migrations in sequence
psql -d borne_map -f database/migrations/0001_create_ev_platform_schema.sql
psql -d borne_map -f database/migrations/0002_create_partner_table.sql
psql -d borne_map -f database/migrations/0003_create_station_table.sql
psql -d borne_map -f database/migrations/0004_create_charger_and_availability_tables.sql
```

Or using `sqlx migrate run` if configured (requires DATABASE_URL):

```bash
DATABASE_URL=postgres://user:pass@localhost:5432/borne_map sqlx migrate run
```

## Seed Data

```bash
# Run seeds in order (TRUNCATE CASCADE + INSERT)
psql -d borne_map -f database/seeds/001_partners.sql
psql -d borne_map -f database/seeds/002_stations.sql
psql -d borne_map -f database/seeds/003_chargers.sql
psql -d borne_map -f database/seeds/004_station_availability.sql
```

## Verify

```bash
# Check row counts
psql -d borne_map -c 'SELECT count(*) FROM "ev-platform".partner;'
psql -d borne_map -c 'SELECT count(*) FROM "ev-platform".station;'
psql -d borne_map -c 'SELECT count(*) FROM "ev-platform".charger;'
psql -d borne_map -c 'SELECT count(*) FROM "ev-platform".station_availability;'
```

Expected output:
```
 count
-------
     3

 count
-------
    15

 count
-------
    24

 count
-------
    15
```

## Verify Constraints

```bash
# These should all FAIL with constraint violation errors:
psql -d borne_map -c 'INSERT INTO "ev-platform".station (id, partner_id, name, latitude, longitude, created_at, created_by, updated_at, updated_by) VALUES ('"'"'X'"'"', '"'"'PRT001'"'"', '"'"'Test'"'"', 100, 0, NOW(), '"'"'tester'"'"', NOW(), '"'"'tester'"'"');'
psql -d borne_map -c 'INSERT INTO "ev-platform".charger (id, station_id, connector_type, power_kw, status, created_at, created_by, updated_at, updated_by) VALUES ('"'"'X'"'"', '"'"'STN001'"'"', '"'"'unknown'"'"', 50, '"'"'available'"'"', NOW(), '"'"'tester'"'"', NOW(), '"'"'tester'"'"');'
psql -d borne_map -c 'INSERT INTO "ev-platform".charger (id, station_id, connector_type, power_kw, status, created_at, created_by, updated_at, updated_by) VALUES ('"'"'X'"'"', '"'"'STN001'"'"', '"'"'type2'"'"', 0, '"'"'available'"'"', NOW(), '"'"'tester'"'"', NOW(), '"'"'tester'"'"');'
```

## Verify Spatial Index

```bash
EXPLAIN ANALYZE SELECT s.id, s.name
FROM "ev-platform".station s
JOIN "ev-platform".partner p ON s.partner_id = p.id
WHERE p.is_verified = true AND p.is_live = true AND p.is_active = true
  AND ST_DWithin(s.location, ST_SetSRID(ST_MakePoint(10.1815, 36.8008), 4326)::geography, 100000);
```

Expected: Query plan shows `Index Scan using idx_station_location` (not `Seq Scan`).
