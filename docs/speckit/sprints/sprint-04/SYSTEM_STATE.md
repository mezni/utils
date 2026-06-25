# SYSTEM_STATE.md — Sprint 04

**Date**: 2026-06-25
**Branch**: `sprint/04-ev-domain-bootstrap`

---

## Platform State

### Database (platform_db)

| Schema | Status | Tables |
|--------|--------|--------|
| `gis` | ✅ Created (Sprint 01) | `osm_charging_stations_temp`, `osm_charging_stations` |
| `ev` | ✅ Created (Sprint 04) | `access_types`, `data_sources`, `connector_types`, `current_types`, `connector_statuses`, `partners`, `stations`, `chargers` |
| `users` | ⏳ Not created | Will be created by auth-service |
| `inventory` | ⏳ Not created | Will be created by admin-service |

### Migrations

| # | File | Status |
|---|------|--------|
| 001 | `migrations/platform_db/ev/001_create_schema.sql` | ✅ Written, applied |
| 002 | `migrations/platform_db/ev/002_lookup_tables.sql` | ✅ Written, applied |
| 003 | `migrations/platform_db/ev/003_create_partners.sql` | ✅ Written, applied |
| 004 | `migrations/platform_db/ev/004_create_stations.sql` | ✅ Written, applied |
| 005 | `migrations/platform_db/ev/005_create_chargers.sql` | ✅ Written, applied |
| 006 | `migrations/platform_db/ev/006_migrate_gis_to_ev.sql` | ✅ Written, applied |

### Extensions

| Extension | Status | Purpose |
|-----------|--------|---------|
| `postgis` | ✅ Installed | GEOGRAPHY type for spatial queries |
| `hstore` | ✅ Installed | Key-value tags on stations |

### Lookup Tables (Seed Data)

| Table | Rows | Values |
|-------|------|--------|
| `ev.access_types` | 3 | public, restricted, private |
| `ev.data_sources` | 3 | osm, partner, manual |
| `ev.connector_types` | 6 | CCS, CHAdeMO, Type 2, Type 1, GB/T, Tesla |
| `ev.current_types` | 2 | AC, DC |
| `ev.connector_statuses` | 4 | available, in_use, offline, faulted |

### Entity Tables

| Table | ID Prefix | Constraint Highlights |
|-------|-----------|----------------------|
| `ev.partners` | OPR-nanoid(12) | partner_type CHECK, UUID audit, soft-delete |
| `ev.stations` | STA-nanoid(12) | GEOGRAPHY(Point,4326), GIST index, HSTORE tags, FK→partners |
| `ev.chargers` | CHG-nanoid(12) | FK→stations(CASCADE), FK→lookups, unique_connector, count constraints |

### Spatial Index

| Index | Type | Table | Column |
|-------|------|-------|--------|
| `idx_stations_location` | GIST | `ev.stations` | `location` |

### GIS → EV Migration

| Direction | Source | Target | Status |
|-----------|--------|--------|--------|
| `gis` → `ev` | `gis.osm_charging_stations` | `ev.stations` | ✅ Idempotent, 0 rows migrated (no source data) |

## Services

| Service | Port | Status |
|---------|------|--------|
| `auth-service` | 3000 | ⏳ Not implemented |
| `driver-service` | 3001 | ✅ Implemented (port 3001) |
| `admin-service` | 3002 | ⏳ Not implemented (ev schema owner) |

## Infrastructure

| Component | Status |
|-----------|--------|
| PostgreSQL | ✅ `postgis/postgis:16-3.4` (PostGIS + hstore enabled) |
| `infra/docker/osm-importer` | ✅ Existing from Sprint 01 |

## Constitution Compliance

| Rule | Status |
|------|--------|
| §2.1 No new services | ✅ |
| §2.4 Entity ID standard | ✅ OPR-, STA-, CHG-nanoid(12) |
| §4.1 Schema ownership | ✅ ev → admin-service |
| §10.3 Identity separation | ✅ UUID audit fields only, no UUID as entity ID |
| §17 Forward-only migrations | ✅ IF NOT EXISTS / ON CONFLICT |
| §19 KNOWN-001 (test data) | ✅ Migration filters is_test = FALSE |
| §19 KNOWN-002 (soft-delete) | ✅ deleted_at on all entities |
| Spatial index | ✅ GIST on ev.stations.location |
