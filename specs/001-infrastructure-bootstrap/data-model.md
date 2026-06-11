# Data Model: Infrastructure Bootstrap

Infrastructure sprint — no application data entities created.

## Databases

| Database | Image | Port (Host) | Purpose |
|---|---|---|---|
| platform_db | postgis/postgis:16-3.4 | 5432 | System of record, PostGIS enabled |
| analytics_db | postgres:16 | 5433 | Append-only event store |

## Schemas (Deferred to Service Sprints)

| Schema | Owner | Sprint |
|---|---|---|
| inventory (partner, station, charger) | admin-service | Sprint 1.1 |
| raw_events | clickstream-service | Sprint 1.3 |
| users | auth-gateway | MVP-3 |

## Seed Data

SQL scripts in `/infra/db/seed/`:
- `001_partners.sql` — 3 initial partners
- `002_stations.sql` — 10 stations (Tunis region)
- `003_chargers.sql` — 30 chargers across stations
