# Tasks — GIS Data & Nearby Discovery

**Feature**: GIS Data & Nearby Discovery — MVP-2 Sprint 2.0
**Last Updated**: 2026-06-16

## Overview

This document provides detailed implementation tasks for the GIS data layer, organized by user stories to enable independent implementation and testing. Tasks are sequential within phases but parallelizable across user stories.

## User Story Mapping

| User Story | Priority | Description | Phase |
|------------|----------|-------------|-------|
| US1 | P1 | Driver sees nearby stations on map | Phase 3 |
| US2 | P1 | Driver knows which stations are active | Phase 4 |
| US3 | P2 | Driver can filter stations by visibility | Phase 5 |
| US4 | P1 | Partner imports data into system | Phase 6 |
| US5 | P2 | Developer runs import process | Phase 7 |

## Implementation Strategy

- **MVP Scope**: US1 only (driver sees nearby stations on map)
- **Incremental Delivery**:
  - MVP-2 Sprint 2.0: US1 (Phase 3) - Core map integration
  - MVP-2 Sprint 2.1: US1 + US2 (Phase 3 + 4) - Active station filtering
  - MVP-2 Sprint 2.2: All stories (Phase 3-7) - Full feature set
- **Parallel Opportunities**: US1, US2, US3 are independent of US4, US5

## Dependencies

### Story Completion Order
```
Phase 1 (Setup) → Phase 2 (Foundational) → Phase 3 (US1) → Phase 4 (US2) → Phase 5 (US3) → Phase 6 (US4) → Phase 7 (US5) → Phase 8 (Polish)
```

### Inter-Story Dependencies
- **US1, US2, US3**: All depend on Phase 2 (Foundational)
- **US4, US5**: Both depend on Phase 2 (Foundational) and can run in parallel
- **US2, US3**: Can run in parallel after US1 (independent filtering logic)
- **Polish**: Depends on all user story phases completing

## Parallel Execution Examples

### Phase 3 (US1) - Driver Sees Nearby Stations

```bash
# Run these in parallel after Phase 2 completes
cd /home/dali/WORK/BorneMap/apps/mobile-driver && npx expo start -c &
cd /home/dali/WORK/BorneMap/apps/web-driver && npm run dev &
cd /home/dali/WORK/BorneMap/services/driver-service && cargo test
```

### Phase 4 (US2 + US3) - Active Stations + Visibility Filter

```bash
# These can run in parallel
cd /home/dali/WORK/BorneMap/services/driver-service && cargo test --test test_active_filter &
cd /home/dali/WORK/BorneMap/services/driver-service && cargo test --test test_visibility_filter &
```

### Phase 6 + Phase 7 (US4 + US5)

```bash
# Can run in parallel (import endpoint + import process container)
docker compose --profile import up osm-importer &
# API endpoint already available from Phase 3
curl -X POST http://localhost:3001/api/v1/import \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"bbox": {"min_lat": 30.0, "min_lon": 7.5, "max_lat": 37.5, "max_lon": 11.6}}'
```

## Tasks

### Phase 1: Setup (Project Initialization)

- [X] T001 Create specs/002-gis-nearby-api directory structure with subdirectories
- [X] T002 Copy plan.md to specs/002-gis-nearby-api/plan.md
- [X] T003 Copy data-model.md to specs/002-gis-nearby-api/data-model.md
- [X] T004 Copy contracts/api.md to specs/002-gis-nearby-api/contracts/api.md
- [X] T005 Copy quickstart.md to specs/002-gis-nearby-api/quickstart.md
- [X] T006 Update AGENTS.md to reference specs/002-gis-nearby-api/plan.md

### Phase 2: Foundational (Blocking Prerequisites)

- [X] T007 Create inventory.station table schema in infra/db/init-platform-db.sql
- [X] T008 Create inventory.charger table schema in infra/db/init-platform-db.sql
- [X] T009 Create gis.import_log table schema in infra/db/init-platform-db.sql
- [X] T010 Add GIST spatial indexes for inventory.station.location
- [X] T011 Add btree indexes for inventory.station (status, visibility, city)
- [X] T012 Add indexes for inventory.charger (station_id, status, connector_type)
- [X] T013 Add indexes for gis.import_log (start_time DESC, status)
- [X] T014 Create gis.nearby() SQL function with ST_DWithin in infra/db/init-platform-db.sql
- [X] T015 Add spatial extension to platform_db in docker-compose.yml

### Phase 3: User Story 1 - Driver Sees Nearby Stations (P1)

**Goal**: Driver can see nearby charging stations on the map

**Independent Test Criteria**:
- Driver app displays station markers on map when panning
- Nearby API returns station data for valid coordinates
- Mobile and web apps render markers correctly

**Implementation Tasks**:
- [X] T016 [US1] Create getNearby() repository method in services/driver-service/src/repository/station_repository.rs
- [X] T017 [US1] Implement coordinate validation (GEO_001) in services/driver-service/src/middleware/validation.rs
- [X] T018 [US1] Create nearby endpoint handler in services/driver-service/src/routes/nearby.rs
- [X] T019 [US1] Add rate limiting middleware (RATE_001) in services/driver-service/src/middleware/rate_limit.rs
- [X] T020 [US1] Add authentication middleware (AUTH_001) in services/driver-service/src/middleware/auth.rs
- [X] T021 [US1] Implement charger query logic in services/driver-service/src/handler/nearby.rs
- [X] T022 [US1] Add error handling (GEO_002, GEO_003) in services/driver-service/src/handler/nearby.rs
- [X] T023 [US1] Create shared types (Station, Charger, NearbyResponse) in packages/shared-types/src/gis.ts
- [X] T024 [US1] Create API client function in packages/api-client/src/nearby.ts
- [X] T025 [US1] Create useNearby hook with debouncing (300ms) in packages/shared-hooks/src/useNearby.ts
- [X] T026 [US1] Add marker rendering for commercial stations in apps/mobile-driver/src/components/StationMarker.tsx
- [X] T027 [US1] Add marker rendering for private home stations in apps/mobile-driver/src/components/StationMarker.tsx
- [X] T028 [US1] Implement marker clustering (zoom < 13) in apps/mobile-driver/src/hooks/useClustering.ts
- [X] T029 [US1] Add map marker display in apps/mobile-driver/src/screens/DriverMapScreen.tsx
- [X] T030 [US1] Add loading state (spinner) in apps/mobile-driver/src/screens/DriverMapScreen.tsx
- [X] T031 [US1] Add error state (retry banner) in apps/mobile-driver/src/screens/DriverMapScreen.tsx
- [X] T032 [US1] Add map marker display in apps/web-driver/src/components/StationMarker.tsx
- [X] T033 [US1] Implement marker clustering in apps/web-driver/src/hooks/useClustering.ts
- [X] T034 [US1] Add map marker display in apps/web-driver/src/screens/Dashboard.tsx
- [X] T035 [US1] Add loading/error states in apps/web-driver/src/screens/DriverMapScreen.tsx

### Phase 4: User Story 2 - Driver Sees Active Stations (P1)

**Goal**: Driver can identify and only see stations that are active

**Independent Test Criteria**:
- API only returns stations with status = 'active'
- Map markers only show active stations
- Soft-deleted stations are not displayed

**Implementation Tasks**:
- [X] T036 [US2] Add status filter to getNearby() in services/driver-service/src/repository/station_repository.rs
- [X] T037 [US2] Add deleted_at IS NULL filter in gis.nearby() SQL function in infra/db/init-platform-db.sql
- [X] T038 [US2] Add unit tests for active station filtering in services/driver-service/tests/test_active_stations.rs
- [X] T039 [US2] Add visual indicator for active stations in apps/mobile-driver/src/components/StationMarker.tsx
- [X] T040 [US2] Add visual indicator for inactive/closed stations in apps/web-driver/src/components/StationMarker.tsx

### Phase 5: User Story 3 - Driver Filters by Visibility (P2)

**Goal**: Driver can filter stations by visibility type

**Independent Test Criteria**:
- API accepts visibility parameter
- API filters by visibility correctly
- Map displays filtered markers

**Implementation Tasks**:
- [X] T041 [US3] Add visibility parameter to getNearby() in services/driver-service/src/repository/station_repository.rs
- [X] T042 [US3] Add visibility filter to SQL query in infra/db/init-platform-db.sql
- [X] T043 [US3] Add visibility filter to nearby endpoint handler in services/driver-service/src/routes/nearby.rs
- [X] T044 [US3] Add visibility filter to API response in services/driver-service/src/handler/nearby.rs
- [X] T045 [US3] Create visibility filter UI in apps/mobile-driver/src/components/VisibilityFilter.tsx
- [X] T046 [US3] Create visibility filter UI in apps/web-driver/src/components/VisibilityFilter.tsx
- [X] T047 [US3] Integrate visibility filter with useNearby hook in packages/shared-hooks/src/useNearby.ts

### Phase 6: User Story 4 - Partner Imports Data (P1)

**Goal**: Partner can trigger data import from OpenStreetMap

**Independent Test Criteria**:
- Import endpoint accepts POST requests
- Import process fetches OSM data
- Import process stores data in database

**Implementation Tasks**:
- [ ] T048 [US4] Create POST /api/v1/import endpoint handler in services/driver-service/src/routes/import.rs
- [ ] T049 [US4] Implement bounding box validation (IMPORT_001) in services/driver-service/src/handler/import.rs
- [ ] T050 [US4] Add admin role check to import endpoint in services/driver-service/src/middleware/auth.rs
- [ ] T051 [US4] Create OSM API fetcher in infra/osm-importer/src/fetcher.rs
- [ ] T052 [US4] Implement JSON transformer in infra/osm-importer/src/transform.rs
- [ ] T053 [US4] Implement database upsert logic in infra/osm-importer/src/import.rs
- [ ] T054 [US4] Add concurrent import prevention with pg_advisory_xact_lock in infra/osm-importer/src/import.rs
- [ ] T055 [US4] Add import logging to gis.import_log table in infra/osm-importer/src/import.rs
- [ ] T056 [US4] Create Dockerfile for osm-importer in infra/osm-importer/Dockerfile
- [ ] T057 [US4] Create docker-compose service entry in infra/docker-compose.yml (osm-importer with import profile)
- [ ] T058 [US4] Add admin JWT generation utility in services/auth-service/src/utils/jwt.rs

### Phase 7: User Story 5 - Developer Runs Import Process (P2)

**Goal**: Developer can run import process to populate database

**Independent Test Criteria**:
- Import process starts successfully
- Import process fetches Tunisia data
- Import process displays import statistics

**Implementation Tasks**:
- [ ] T059 [US5] Implement OSM API fetcher with bounding box query in infra/osm-importer/src/fetcher.rs
- [ ] T060 [US5] Add chunked fetching for large regions in infra/osm-importer/src/fetcher.rs
- [ ] T061 [US5] Add import retry logic (max 3 retries) in infra/osm-importer/src/fetcher.rs
- [ ] T062 [US5] Add error handling for OSM timeout (IMPORT_001) in infra/osm-importer/src/fetcher.rs
- [ ] T063 [US5] Add error handling for DB connection failure in infra/osm-importer/src/import.rs
- [ ] T064 [US5] Add import progress logging to stdout in infra/osm-importer/src/import.rs
- [ ] T065 [US5] Add import summary (stations_imported, stations_updated, stations_failed) to output in infra/osm-importer/src/import.rs
- [ ] T066 [US5] Add documentation in infra/osm-importer/README.md (how to run, env vars, expected output)
- [ ] T067 [US5] Add environment variables to .env.example (DATABASE_URL, OVERPASS_URL, BOUNDING_BOX)
- [ ] T068 [US5] Add import logs query in infra/osm-importer/src/logs.rs (for debugging)

### Phase 8: Polish & Cross-Cutting Concerns

**Goal**: Testing, documentation, and final validation

**Implementation Tasks**:
- [ ] T069 [P] Add unit tests for gis.nearby() SQL function in infra/db/tests/test_gis_function.rs
- [ ] T070 [P] Add integration tests for nearby endpoint in services/driver-service/tests/test_nearby_api.rs
- [ ] T071 [P] Add integration tests for import endpoint in services/driver-service/tests/test_import_api.rs
- [ ] T072 [P] Add integration tests for import process in infra/osm-importer/tests/test_import.rs
- [ ] T073 [P] Create performance tests (1000+ stations in radius) in services/driver-service/tests/test_performance.rs
- [ ] T074 [P] Add TypeScript strict type checking for shared packages in package.json
- [ ] T075 [P] Add ESLint config for driver apps in apps/mobile-driver/.eslintrc.js
- [ ] T076 [P] Update README.md with GIS feature description
- [ ] T077 [P] Add swagger documentation for API endpoints in services/driver-service/src/swagger.rs
- [ ] T078 [P] Add error code documentation in docs/error-codes.md
- [ ] T079 [P] Add database seed script in infra/db/seed-tunisia.sql

## Task Statistics

- **Total Tasks**: 79
- **Setup Phase**: 6 tasks (Phase 1)
- **Foundational Phase**: 9 tasks (Phase 2)
- **User Story 1**: 20 tasks (Phase 3)
- **User Story 2**: 5 tasks (Phase 4)
- **User Story 3**: 7 tasks (Phase 5)
- **User Story 4**: 11 tasks (Phase 6)
- **User Story 5**: 10 tasks (Phase 7)
- **Polish Phase**: 11 tasks (Phase 8)

## MVP Scope

**Minimum Viable Product** (Phase 3 only):
- 20 tasks
- T001-T016 (Setup + Foundational)
- T016-T035 (User Story 1 - Driver sees nearby stations)

**Full Feature Set** (All phases):
- 79 tasks
- All user stories implemented

## Testing Strategy

### Unit Tests
- SQL function tests (gis.nearby)
- Repository method tests (getNearby)
- Handler validation tests (coordinate validation)
- Import logic tests (upsert, retry, logging)

### Integration Tests
- Nearby API endpoint tests (GET /api/v1/nearby)
- Import API endpoint tests (POST /api/v1/import)
- Import process end-to-end tests
- Database query tests

### Manual Tests
- Map marker rendering (mobile + web)
- Marker clustering (zoom levels)
- Loading and error states
- Import process execution
- Spatial query performance

## Performance Targets

- Spatial query latency: < 5 seconds (NFR1)
- API response time: < 1 second
- Marker rendering: < 100ms
- Import process time: < 10 minutes
- Rate limit: 100 queries/minute

## Open Issues

None. All clarifications resolved in `/speckit.clarify`.
