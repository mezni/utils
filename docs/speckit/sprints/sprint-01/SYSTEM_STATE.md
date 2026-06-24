# SYSTEM STATE — Sprint 01

**Date**: 2026-06-24
**Sprint**: 01 — Bootstrap platform_db + GIS ingestion + OSM Tunisia import + geospatial query function

---

## Project Structure

```
bornemap/
├── infra/
│   ├── docker/
│   │   ├── docker-compose.yml          # PostGIS + OSM importer
│   │   └── osm-importer/
│   │       ├── Dockerfile              # Python 3.12-based OSM ETL
│   │       ├── requirements.txt        # psycopg2, osmium, requests
│   │       ├── entrypoint.sh           # Container entrypoint
│   │       └── import_osm.py           # OSM PBF parser + ETL pipeline
│   ├── postgres/
│   │   └── migrations/
│   │       ├── 001_create_gis_schema.sql
│   │       ├── 002_create_staging_table.sql
│   │       ├── 003_create_curated_table.sql
│   │       └── 004_create_find_nearby_stations.sql
│   └── tests/
│       ├── test_schema_validation.sql  # SQL-level validation
│       └── test_integration.sh         # Docker + function integration test
├── docs/
│   ├── speckit/
│   │   └── sprints/
│   │       └── sprint-01/              # Sprint artifacts (this directory)
│   └── governance/                     # (future sprints)
├── source/                             # (future sprints — runtime code)
└── .specify/                           # Speckit workflow system
```

## Database State

| Schema | Table | Status |
|--------|-------|--------|
| `gis` | `osm_charging_stations_temp` | ✅ Created |
| `gis` | `osm_charging_stations` | ✅ Created |
| `gis` | `find_nearby_stations(...)` | ✅ Created |

## Docker Services

| Service | Image | Status |
|---------|-------|--------|
| `postgres` | postgis/postgis:16-3.4 | ✅ Configured |
| `osm-importer` | custom (Dockerfile) | ✅ Built & ready |

## Pending Dependencies

| Item | Blocked By | Expected Sprint |
|------|-----------|-----------------|
| SQLx compile validation | driver-service Cargo.toml | Sprint 02+ |
| Service integration | driver-service initialization | Sprint 02+ |
| `deleted_at` on curated table | Known bug KNOWN-002 | Sprint 02+ |

## Function Registry

### `gis.find_nearby_stations(lat, lon, radius, limit_count)`

- **Parameters**: lat (DP), lon (DP), radius=5000 (meters), limit_count=50
- **Returns**: station_id, name, lat, lon, distance_km
- **Algorithm**: Haversine (no PostGIS dependency required)
- **Ordering**: ASC by distance_km
- **Safety**: NULL lat/lon excluded, non-null distances guaranteed
