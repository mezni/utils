# SYSTEM_STATE.md — Sprint 01

**Date**: 2026-06-24
**Branch**: `001-bootstrap-gis-ingestion`

---

## Platform State

### Database (platform_db)

| Schema | Status | Tables |
|--------|--------|--------|
| `gis` | ✅ Created | `osm_charging_stations_temp`, `osm_charging_stations` |
| `users` | ⏳ Not created | Will be created by auth-service |
| `inventory` | ⏳ Not created | Will be created by admin-service |

### Migrations

| # | File | Status |
|---|------|--------|
| 001 | `migrations/platform_db/gis/001_create_schema.sql` | ✅ Written |
| 002 | `migrations/platform_db/gis/002_create_staging_table.sql` | ✅ Written |
| 003 | `migrations/platform_db/gis/003_create_curated_table.sql` | ✅ Written |
| 004 | `migrations/platform_db/gis/004_find_nearby_stations.sql` | ✅ Written |

### SQL Function

| Function | Status | Description |
|----------|--------|-------------|
| `gis.find_nearby_stations(lat, lon, radius, limit)` | ✅ Written | Haversine-based geospatial query |

### OSM Importer (Docker)

| Component | Status |
|-----------|--------|
| `infra/docker/osm-importer/Dockerfile` | ✅ Written, image builds |
| `infra/docker/osm-importer/requirements.txt` | ✅ Written |
| `infra/docker/osm-importer/scripts/import.sh` | ✅ Written |
| `infra/docker/osm-importer/scripts/parse_and_import.py` | ✅ Written, syntax valid |

### Known Bug Fixes

| Bug ID | Fix | Status |
|--------|-----|--------|
| KNOWN-001 | `is_test BOOLEAN DEFAULT false` | ✅ Applied in curated table |
| KNOWN-002 | `deleted_at TIMESTAMPTZ` | ✅ Applied in curated table |

## Services

| Service | Port | Status |
|---------|------|--------|
| `auth-service` | 3000 | ⏳ Not implemented |
| `driver-service` | 3001 | ⏳ Not implemented (gis schema owner) |
| `admin-service` | 3002 | ⏳ Not implemented |

## Infrastructure

| Component | Status |
|-----------|--------|
| `infra/docker/` | ✅ osm-importer scaffold |
| PostGIS | 📋 Optional dependency for function |

## Constitution Compliance

| Rule | Status |
|------|--------|
| §2.1 No new services | ✅ |
| §2.4 Entity ID standard | ✅ `STA-nanoid(12)` |
| §4.1 Schema ownership | ✅ gis → driver-service |
| §10.3 Identity separation | ✅ No UUID on entities |
| §14 SQLx enforcement | ⏳ Pending Rust service setup |
| §17 Forward-only migrations | ✅ IF NOT EXISTS / OR REPLACE |
