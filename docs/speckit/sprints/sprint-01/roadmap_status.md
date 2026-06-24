# Roadmap Status — Sprint 01

**Date**: 2026-06-24

---

| Sprint | Theme | Status | Delivery |
|--------|-------|--------|----------|
| **Sprint 01** | Database bootstrap + GIS foundation | ✅ **COMPLETE** | Migrations, Docker, SQL function |
| Sprint 02 | *driver-service initialization* | 🔲 Planned | — |
| Sprint 03 | *auth-service initialization* | 🔲 Planned | — |
| Sprint 04 | *admin-service initialization* | 🔲 Planned | — |
| Sprint 05 | *Frontend foundation* | 🔲 Planned | — |

## Completed Milestones

- [x] PostgreSQL schema `gis` created
- [x] Staging table for OSM raw data
- [x] Curated table with STA-nanoid(12) identity
- [x] OSM importer Docker container (batch ETL)
- [x] `find_nearby_stations` geospatial function
- [x] Integration test suite
- [x] SQL migration validation

## Next Planned Milestones

- Initialize driver-service Rust project
- Integrate SQLx migrations
- Expose GIS data via API
- Implement Redis caching

## Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| SQLx not yet validated (no Rust project) | Medium | Deferred to Sprint 02 |
| OSM PBF download ~50MB+ | Low | Idempotent download, retries |
| PostGIS not in base PostgreSQL image | Low | Using postgis/postgres:16-3.4 |
