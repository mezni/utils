# Schema Notes

## MVP-1 Database
- PostgreSQL is the backing database
- no PostGIS yet
- no spatial indexes yet

## Canonical Rules
- station source of truth is `inventory.station`
- `gis` never owns business entities
- analytics data belongs in `analytics` only

## Planned Migration Approach
- MVP-1 uses the Python service migrations
- MVP-2 introduces canonical SQL migrations under `database/migrations/`
