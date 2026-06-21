# Sprint 2 — GIS Engine Foundation

**Status**: NOT_STARTED
**Constitution Version**: 1.15.2
**Dependencies**: Sprint 1 (identity system live, JWT validation working)

---

## Must Have (Exit Criteria)

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S2-001 | Implement OSM ingestion pipeline in driver-service (batch import, idempotent) | team | NOT_STARTED |
| S2-002 | Create `gis.osm_charging_stations_temp` table (staging) | team | NOT_STARTED |
| S2-003 | Create `gis.osm_charging_stations` table (curated) | team | NOT_STARTED |
| S2-004 | Implement staging → curated ETL pipeline | team | NOT_STARTED |
| S2-005 | Implement PostGIS spatial query engine (nearby, bounding box, radius) | team | NOT_STARTED |
| S2-006 | Implement `GET /api/v1/driver/nearby` endpoint | team | NOT_STARTED |
| S2-007 | Implement `GET /api/v1/driver/station/:id` endpoint | team | NOT_STARTED |
| S2-008 | Create materialized views (mv_stations_geo, mv_stations_summary) | team | NOT_STARTED |
| S2-009 | Set up Redis cache layer (geo:radius, geo:tile keys) | team | NOT_STARTED |
| S2-010 | Implement Redis spatial cache read/write in driver-service | team | NOT_STARTED |
| S2-011 | Implement map rendering API contract (domain-types DTOs) | team | NOT_STARTED |
| S2-012 | Implement GIS data normalization layer (OSM tag → internal schema) | team | NOT_STARTED |
| S2-013 | Implement station clustering support in spatial queries | team | NOT_STARTED |

## Should Have

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S2-014 | Create mobile map view (Expo SDK 54 + markers + clustering) | team | NOT_STARTED |
| S2-015 | Create web map view (React + Leaflet + station popups) | team | NOT_STARTED |
| S2-016 | Implement user location pin on mobile | team | NOT_STARTED |
| S2-017 | Create CI GIS ownership gate | team | NOT_STARTED |
| S2-018 | Create CI spatial query safety gate | team | NOT_STARTED |
| S2-019 | Create CI Redis access gate | team | NOT_STARTED |
| S2-020 | Create CI OSM reproducibility gate | team | NOT_STARTED |
| S2-021 | Create CI map API contract gate | team | NOT_STARTED |

## Nice to Have

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S2-022 | Create `tools/import_osm.sh` reproducibility script | team | NOT_STARTED |
| S2-023 | Add station filtering UI (web + mobile) | team | NOT_STARTED |
| S2-024 | Add basic distance-to-user indicator | team | NOT_STARTED |

## CI Additions (Sprint 2)

| ID | Gate | Rule |
|----|------|------|
| CI-2.1 | GIS Ownership Gate | FAIL if any service other than driver-service writes to gis schema |
| CI-2.2 | Spatial Query Safety Gate | FAIL if raw SQL string used in spatial queries or non-SQLx queries in driver-service |
| CI-2.3 | Redis Access Gate | FAIL if Redis accessed outside driver-service |
| CI-2.4 | OSM Reproducibility Gate | FAIL if ingestion pipeline is non-deterministic or missing idempotency key |
| CI-2.5 | Map API Contract Gate | FAIL if API response deviates from domain-types contract |

## Exit Criteria

Sprint 2 is COMPLETE ONLY IF:
- [ ] OSM ingestion works deterministically (batch, idempotent)
- [ ] PostGIS queries return correct spatial results
- [ ] `/nearby` endpoint fully functional with contract enforced
- [ ] Redis caching operational and isolated to driver-service
- [ ] GIS ownership gate passes
- [ ] Spatial query safety enforced
- [ ] Ingestion reproducibility verified
