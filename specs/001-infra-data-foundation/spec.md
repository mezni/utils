# Feature Specification: MVP-1 Sprint 0 — Infrastructure & Data Foundation

**Feature Branch**: `002-infra-data-foundation`

**Created**: 2026-06-13

**Status**: Draft

**Input**: Sprint 0 from MVP-1 Discovery Core - Setup database infrastructure, enable PostGIS, and seed real EV station data from OpenStreetMap Tunisia extract

---

## User Scenarios & Testing

### User Story 1 - DevOps Engineer Sets Up Docker Infrastructure (Priority: P1)

A DevOps engineer needs to provision the complete database infrastructure for MVP-1 using Docker Compose, ensuring both platform and analytics databases are ready for development and data import.

**Why this priority**: This is the absolute foundation - no other work can proceed without databases running. It's the critical path item that blocks all downstream development.

**Independent Test**: Can be fully tested by running `docker compose up` and verifying both databases are accessible, PostGIS is enabled, and schemas exist via SQL queries.

**Acceptance Scenarios**:

1. **Given** no containers are running, **When** engineer runs `docker compose up -d`, **Then** two PostgreSQL containers (platform_db and analytics_db) start successfully and expose their ports
2. **Given** platform_db is running, **When** engineer executes `CREATE EXTENSION postgis;`, **Then** PostGIS extension is installed and ready for use
3. **Given** PostGIS is enabled, **When** engineer creates the `inventory` and `gis` schemas, **Then** both schemas exist and are accessible
4. **Given** all infrastructure is running, **When** engineer runs `SELECT version();` on both databases, **Then** PostgreSQL responds with version info

---

### User Story 2 - Data Engineer Imports OSM Station Data (Priority: P1)

A data engineer needs to download Tunisia's OpenStreetMap extract, filter for charging stations (amenity=charging_station), convert the data to PostGIS format, and seed the database with 50–300 real stations.

**Why this priority**: P1 because without real data seeded, the geospatial system cannot be validated. This is essential for testing the PostGIS queries and ensuring the nearby search algorithm works correctly.

**Independent Test**: Can be fully tested by verifying that 50+ stations exist in `inventory.station` table, have correct PostGIS GEOGRAPHY columns, and PostGIS distance queries return expected results.

**Acceptance Scenarios**:

1. **Given** Geofabrik Tunisia OSM extract is downloaded, **When** engineer filters for `amenity=charging_station`, **Then** at least 50 charging stations are identified
2. **Given** filtered OSM data exists, **When** engineer converts to SQL INSERT format with PostGIS GEOGRAPHY points, **Then** conversion completes without errors
3. **Given** conversion is complete, **When** engineer executes INSERT statements on `inventory.station`, **Then** all 50–300 records are successfully inserted with id, name, status, latitude, longitude, and location fields
4. **Given** stations are inserted, **When** engineer verifies data integrity with `SELECT COUNT(*) FROM inventory.station WHERE status='active';`, **Then** active stations count is ≥50

---

### User Story 3 - QA Engineer Validates PostGIS Geospatial Queries (Priority: P1)

A QA engineer needs to validate that PostGIS is correctly configured, spatial indexing is in place, and core geospatial queries (ST_DWithin, distance ordering) work correctly with real data.

**Why this priority**: P1 because this validation unlocks Sprint 1 backend development. If PostGIS queries don't work, the entire geospatial engine fails.

**Independent Test**: Can be fully tested by running the standard nearby query (ST_DWithin with ordering) against seeded data and verifying results are accurate, distances are calculated correctly, and query latency is acceptable.

**Acceptance Scenarios**:

1. **Given** 50+ stations are seeded with GEOGRAPHY columns, **When** engineer runs `ST_DWithin` query with test coordinates, **Then** query returns stations within specified radius (e.g., 5000m)
2. **Given** ST_DWithin query returns results, **When** results are examined, **Then** stations are ordered by distance (ascending) from query point
3. **Given** a GIST index exists on the location column, **When** engineer checks `\d inventory.station` in psql, **Then** index `idx_station_location` is present and type is GIST
4. **Given** PostGIS index is in place, **When** engineer runs nearby query on test data, **Then** query completes in <200ms on local development machine

---

### Edge Cases

- What happens if Geofabrik extract is corrupted or unavailable? → Engineer will retry download or use backup source
- What if PostgreSQL container fails to start due to port conflict? → Engineer checks port availability and adjusts docker-compose.yml
- What if OSM data contains duplicate station IDs? → Data import process includes deduplication logic
- What if PostGIS extension installation fails? → Docker image already includes postgis; if extension fails, container logs will indicate dependency issue

---

## Requirements

### Functional Requirements

- **FR-001**: System MUST start platform_db PostgreSQL container with PostGIS extension pre-installed via docker-compose, using database credentials from `.env` file (not hardcoded)
- **FR-002**: System MUST create `inventory` schema in platform_db for station data
- **FR-003**: System MUST create `gis` schema (read-only, reserved for future use) in platform_db
- **FR-004**: System MUST create `inventory.station` table with columns: id, name, status, latitude, longitude, location (GEOGRAPHY POINT)
- **FR-005**: System MUST create a GIST spatial index on inventory.station.location for PostGIS query performance
- **FR-006**: System MUST download and filter OpenStreetMap Tunisia extract for amenity=charging_station
- **FR-007**: System MUST convert filtered OSM data to SQL INSERT statements with PostGIS GEOGRAPHY(POINT, 4326) format
- **FR-008**: System MUST seed inventory.station with 50–300 real charging stations from Tunisia OSM data
- **FR-009**: System MUST support ST_DWithin PostGIS queries on seeded station data
- **FR-010**: System MUST start analytics_db PostgreSQL container (empty, reserved for MVP-4)
- **FR-011**: System MUST persist database data via Docker volumes so data survives container restarts
- **FR-012**: System MUST provide `.env.example` template with required environment variables and dev defaults for local development

### Key Entities

- **Station**: Represents a real EV charging station with id (STA-xxx or OSM-derived), name, status (active/maintenance), latitude, longitude, and location GEOGRAPHY point. Source: OpenStreetMap. Read-only in Sprint 0.
- **Platform Database (platform_db)**: PostgreSQL + PostGIS instance containing inventory and gis schemas. System of record for all geospatial and operational data.
- **Analytics Database (analytics_db)**: PostgreSQL instance (empty in Sprint 0). Reserved for append-only analytics events in MVP-4.

---

## Success Criteria

### Measurable Outcomes

- **SC-001**: Docker compose infrastructure can be spun up from cold start in under 2 minutes with zero manual intervention
- **SC-002**: PostGIS extension is automatically enabled and `ST_DWithin` queries execute in <200ms on developer laptops
- **SC-003**: OpenStreetMap Tunisia extract is successfully filtered to 50–300 charging stations and seeded into inventory.station
- **SC-004**: All 50–300 seeded stations have valid GEOGRAPHY points and can be queried via PostGIS distance queries with accurate results (±1% distance calculation)
- **SC-005**: Database schemas (inventory, gis) exist and are accessible to backend service with correct read/write permissions
- **SC-006**: GIST spatial index on location column reduces query latency by >50% compared to non-indexed queries (benchmarked)

---

## Assumptions

- **Docker Environment**: Development team has Docker and Docker Compose installed and working on their machines
- **OSM Data Availability**: Geofabrik Tunisia OSM extracts are publicly available and remain stable throughout Sprint 0
- **Database Credentials**: Default credentials (bornemap/bornemap_dev for platform_db) are managed via `.env` file with sensible dev defaults; environment-specific overrides (CI/staging) supported via environment variables, production credentials deferred to MVP-5+
- **Storage Capacity**: Developer machines have sufficient disk space for Docker volumes (50–100GB expected)
- **Network Access**: Developers have internet access to download OSM extracts from Geofabrik
- **No Authentication in Sprint 0**: Database setup does not include Keycloak or OAuth2; that's deferred to MVP-3
- **PostGIS Version**: postgis/postgis:16-3.4 Docker image includes all necessary extensions and is stable
- **Data Schema Stability**: inventory.station schema will not change significantly during MVP-1; future MVP changes will be backward-compatible migrations

---

## Clarifications

### Session 2026-06-13

- Q: Should Sprint 0 support environment-specific database credentials (dev/CI/staging)? → A: Use `.env` file for all credentials with dev defaults; enables CI/staging override without modifying docker-compose.yml; follows Docker best practices; prevents accidental credential commits to VCS.

**Impact on Implementation:**
- **FR-001** updated: docker-compose.yml MUST reference credential variables from `.env` file, not hardcode credentials
- **New Requirement Added**: Sprint 0 MUST include `.env.example` template showing required environment variables with dev defaults
- **New Acceptance Scenario (Story 1)**: Given a fresh clone of the repository, when engineer copies `.env.example` to `.env`, then `docker compose up -d` starts containers with dev credentials without modification

---

## Context & Rationale

Sprint 0 is the critical infrastructure phase of MVP-1. It establishes the **source of truth** for all geospatial data (OSM-derived real stations in PostGIS) and ensures PostGIS queries are validated before backend development begins in Sprint 1.

Without real data, the backend and frontend cannot be properly validated. PostGIS performance testing in this sprint informs latency targets for Sprint 1+ development.

**System View**:
```
OSM Tunisia → Filter & Transform → PostGIS Insert → inventory.station → (validated for Sprint 1)
```

---

*This specification defines Sprint 0: Infrastructure & Data Foundation. Completion of this sprint unblocks Sprint 1 (backend driver-service development).*
