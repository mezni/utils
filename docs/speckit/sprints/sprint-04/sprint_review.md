# Sprint Review — Sprint 04

**Date**: 2026-06-25
**Branch**: `sprint/04-ev-domain-bootstrap`

---

## Sprint Goal

EV Domain Bootstrap: Create the canonical EV domain model with `ev` schema, lookup tables, partners, stations (with PostGIS), chargers, and GIS → EV migration pipeline.

## Deliverables

### Migrations Created

| # | File | Lines | Description |
|---|------|-------|-------------|
| 001 | `ev/001_create_schema.sql` | 4 | Schema + PostGIS + hstore |
| 002 | `ev/002_lookup_tables.sql` | 82 | 5 lookup tables + seed data |
| 003 | `ev/003_create_partners.sql` | 13 | OPR-nanoid(12) partners |
| 004 | `ev/004_create_stations.sql` | 21 | STA-nanoid(12) + GEOGRAPHY + GIST |
| 005 | `ev/005_create_chargers.sql` | 22 | CHG-nanoid(12) + constraints |
| 006 | `ev/006_migrate_gis_to_ev.sql` | 15 | Idempotent GIS → EV migration |

### Validation Artifacts

- `tests/ev/001_schema_test.sql` — 11 schema existence tests
- `tests/ev/002_id_format_test.sql` — 3 ID format tests
- `tests/ev/003_constraint_test.sql` — 12 constraint + data tests

### Infrastructure Change

- Docker compose: `postgres:16-alpine` → `postgis/postgis:16-3.4`

## Key Decisions

1. **PostGIS required**: GEOGRAPHY(Point,4326) mandates PostGIS. The docker image was upgraded from `postgres:16-alpine` to `postgis/postgis:16-3.4`.
2. **No SQLx validation**: The ev schema is owned by admin-service which doesn't exist yet. SQLx compile validation will happen when admin-service is implemented.
3. **GIS → EV migration uses MD5-based nanoid**: Since no runtime service exists to generate nanoid, migration 006 uses `md5(random()::text)` truncated to 12 chars. This produces unique IDs per row. A production service would use the proper nanoid library.

## Scope Compliance

| Domain | Status |
|--------|--------|
| ev schema | ✅ Created |
| Lookup tables | ✅ 5 tables with seed data |
| ev.partners | ✅ Created |
| ev.stations + spatial index | ✅ Created with PostGIS |
| ev.chargers + constraints | ✅ Created |
| GIS → EV migration | ✅ Idempotent |
| API endpoints | ✅ None (scope respected) |
| Frontend | ✅ None (scope respected) |
| New services | ✅ None (scope respected) |

## Known Issues

| Issue | Severity | Notes |
|-------|----------|-------|
| No stations in GIS | LOW | OSM Tunisia dataset has 0 charging stations. Migration runs correctly but migrates 0 rows. |
| SQLx validation deferred | MEDIUM | Requires admin-service implementation |
| Nanoid in SQL is MD5-based | LOW | Production nanoid generation will use proper library |

## Test Results

**26/26 tests passing** — all schema, constraint, identity, spatial, and FK validations pass.
