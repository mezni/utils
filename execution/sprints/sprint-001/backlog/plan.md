# Sprint 001 — Implementation Plan
**Version:** 1.0
**Date:** June 2026
**Phase:** PLAN

---

## Epics

### EPIC-001 — Infrastructure & Schema (Features 1–2)
**Goal:** PostgreSQL 16 + PostGIS running, extensions loaded, full inventory schema deployed.

**User Stories:**
- US1: As the system, I need PostGIS running with extensions so spatial data can be stored and queried.
- US2: As the system, I need lookup tables seeded so entity tables can reference valid statuses and types.
- US3: As the system, I need entity tables (partners, stations, chargers, connectors) created with nanoid PKs, soft-delete, and audit columns.

### EPIC-002 — OSM Data Pipeline (Features 3–4)
**Goal:** Raw OSM Tunisia data ingested and normalized into `inventory.stations`.

**User Stories:**
- US4: As a system, I want OSM charging station data imported from Tunisia into a staging table.
- US5: As a system, I want raw staging records transformed into canonical `inventory.stations` with nanoid IDs, GEOGRAPHY geometry, and HSTORE tags.

### EPIC-003 — Spatial Query & API (Features 5–6)
**Goal:** PostGIS spatial function + REST API exposing nearby stations.

**User Stories:**
- US6: As a driver, I want a spatial query function that returns stations sorted by distance.
- US7: As a frontend, I need a REST endpoint `GET /api/v1/driver/nearby` returning JSON.

### EPIC-004 — Health & Map UI (Features 7–8)
**Goal:** Service observability + driver-facing map visualization.

**User Stories:**
- US8: As an operator, I want a health endpoint so I can verify service availability.
- US9: As a driver, I want to see nearby charging stations on a Leaflet map.

---

## Tasks by Epic

### EPIC-001 — Infrastructure & Schema

| ID | Task | Depends On | File Path |
|---|---|---|---|
| T001 | Create docker-compose.yml with PostgreSQL 16 + PostGIS service | — | `platform/infrastructure/docker/docker-compose.yml` |
| T002 | Create init SQL script with extensions (postgis, hstore, pgcrypto) | T001 | `platform/infrastructure/postgres/init/01-extensions.sql` |
| T003 | Create lookup table migrations (access_types, data_sources, connector_types, current_types, statuses) | T002 | `platform/infrastructure/postgres/migrations/02-lookup-tables.sql` |
| T004 | Seed lookup tables with initial data | T003 | `platform/infrastructure/postgres/seeds/03-lookup-seeds.sql` |
| T005 | Create partners table migration (PAR-nanoid, soft-delete, audit) | T003 | `platform/infrastructure/postgres/migrations/04-partners.sql` |
| T006 | Create stations table migration (STA-nanoid, GEOGRAPHY, HSTORE, is_test, soft-delete) | T005 | `platform/infrastructure/postgres/migrations/05-stations.sql` |
| T007 | Create chargers table migration (CHR-nanoid, FK to stations) | T006 | `platform/infrastructure/postgres/migrations/06-chargers.sql` |
| T008 | Create connectors table migration (CON-nanoid, FK to chargers, UNIQUE constraint) | T007 | `platform/infrastructure/postgres/migrations/07-connectors.sql` |
| T009 | Create gis.osm_charging_stations_temp staging table | T002 | `platform/infrastructure/postgres/migrations/08-osm-staging.sql` |
| T010 | Verify: `docker compose up`, `SELECT PostGIS_version()`, all tables exist | T001–T009 | — |

### EPIC-002 — OSM Data Pipeline

| ID | Task | Depends On | File Path |
|---|---|---|---|
| T011 | Create import.sh script (Overpass API fetch, Tunisia bbox) | T009 | `platform/scripts/import.sh` |
| T012 | Create sync_osm_charging_stations() PL/pgSQL function | T009, T006 | `platform/infrastructure/postgres/functions/10-sync-osm-stations.sql` |
| T013 | Run import + sync, verify stations populated with valid STA-nanoid IDs | T011, T012 | — |

### EPIC-003 — Spatial Query & API

| ID | Task | Depends On | File Path |
|---|---|---|---|
| T014 | Create find_nearby_stations() PL/pgSQL function (ST_DWithin, GEOGRAPHY, is_test filter, distance sort) | T006 | `platform/infrastructure/postgres/functions/11-find-nearby-stations.sql` |
| T015 | Initialize driver-service Rust crate (Actix-web, SQLx, Cargo.toml) | T001 | `source/services/driver-service/Cargo.toml` |
| T016 | Create driver-service domain layer (Station model, NearbyQuery value object) | T015 | `source/services/driver-service/domain/` |
| T017 | Create driver-service infrastructure DB adapter (SQLx query to find_nearby_stations) | T016, T014 | `source/services/driver-service/infrastructure/` |
| T018 | Create driver-service application layer (NearbyStations use-case) | T017 | `source/services/driver-service/application/` |
| T019 | Create driver-service API handler for GET /api/v1/driver/nearby | T018 | `source/services/driver-service/api/` |
| T020 | Create OpenAPI spec for nearby endpoint | T019 | `sprints/sprint-001/api/openapi.yaml` |
| T021 | Verify: API returns real stations from DB | T019, T020 | — |

### EPIC-004 — Health & Map UI

| ID | Task | Depends On | File Path |
|---|---|---|---|
| T022 | Create driver-service API handler for GET /api/v1/driver/health (DB ping) | T015 | `source/services/driver-service/api/health.rs` |
| T023 | Create Traefik docker-compose config (route /api/v1/driver/* → driver-service:3001) | T022 | `platform/infrastructure/traefik/dynamic.yml` |
| T024 | Initialize web app (React + Tailwind + Leaflet, package.json) | — | `source/apps/web/package.json` |
| T025 | Create Leaflet map component (center: Tunisia 34.0, 9.5) | T024 | `source/apps/web/src/components/MapView.tsx` |
| T026 | Create API fetch hook (GET /api/v1/driver/nearby, lat/lng/radius params) | T025 | `source/apps/web/src/hooks/useNearbyStations.ts` |
| T027 | Integrate map + API: render station markers, loading/error/empty states | T026 | `source/apps/web/src/pages/Home.tsx` |
| T028 | Verify: end-to-end — Docker stack up → services healthy → map shows markers | T023, T027 | — |

---

## Dependency DAG

```
T001
 └── T002
      ├── T003 ── T004
      │    ├── T005 ── T006 ── T007 ── T008
      │    └── T009
      └── T015 ── T016 ── T017 ── T018 ── T019 ── T020
           │                              └── T022
           └── T023
T011 (depends on T009)
T012 (depends on T009, T006)
T013 (depends on T011, T012)
T014 (depends on T006)
T024 ── T025 ── T026 ── T027
T028 (depends on T023, T027)
```

## Execution Phases

| Phase | Epic | Tasks | Criteria |
|---|---|---|---|
| Phase 1 | EPIC-001 | T001–T010 | DB boots, all tables exist |
| Phase 2 | EPIC-002 | T011–T013 | OSM stations in inventory.stations |
| Phase 3 | EPIC-003 | T014–T021 | API returns nearby stations |
| Phase 4 | EPIC-004 | T022–T028 | Health OK + map shows markers |

## Sprint State

```json
{
  "sprint_id": "sprint-001",
  "phase": "PLAN",
  "epics": ["EPIC-001", "EPIC-002", "EPIC-003", "EPIC-004"],
  "tasks": {
    "total": 28,
    "blocked": 0,
    "in_progress": 0,
    "completed": 0
  },
  "current_epic": "EPIC-001",
  "current_phase": 1
}
```
