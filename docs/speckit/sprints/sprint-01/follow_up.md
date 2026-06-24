# Follow-Up Queue — Sprint 01

**Date**: 2026-06-24

---

## HIGH (Must address in next sprint)

| ID | Item | Category | Root Cause | Sprint |
|----|------|----------|------------|--------|
| FUP-001 | SQLx compile validation cannot run without Rust service | Technical Debt | No Cargo.toml / sqlx-data.json exists | Sprint 02 |
| FUP-002 | KNOWN-002: `deleted_at` missing on `gis.osm_charging_stations` | Inherited Bug | Constitution v1.15.2 known issue | Sprint 02 |
| FUP-003 | KNOWN-001: Test stations may leak without `is_test = FALSE` filter | Inherited Bug | KNOWN-001 from Constitution | Sprint 02 |

## MEDIUM

| ID | Item | Category | Notes |
|----|------|----------|-------|
| FUP-004 | Add PostGIS-based variant of `find_nearby_stations` | Optimization | Haversine works but PostGIS is more accurate |
| FUP-005 | Document schema ownership for driver-service | Documentation | gis schema owner is driver-service per Constitution |
| FUP-006 | Create `.env.example` for Docker credentials | Engineering | Environment variables currently hardcoded in docker-compose |

## LOW

| ID | Item | Category | Notes |
|----|------|----------|-------|
| FUP-007 | Index on `imported_at` for staging table | Optimization | Useful for batch cleanup queries |
| FUP-008 | Add `docker compose down` to test cleanup | Improvement | Test script currently leaves postgres running |
| FUP-009 | Validate OSM tag filtering (additional charging tags) | Risk | `amenity=charging_station` is primary; may need `socket:type*` tags |
