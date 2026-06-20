# Feature Specification: EV Charging Platform Foundation

**Feature Branch**: `001-ev-charging-foundation`

**Created**: 2026-06-20

**Status**: Draft

**Input**: User description: "sprint 001"

## User Scenarios & Testing

### User Story 1 — Database + Docker Compose infrastructure (Priority: P1)

As a developer, I want to provision a PostGIS-enabled database and Docker Compose setup so that all services have a reproducible foundation to run on.

**Why this priority**: Everything else depends on the database and orchestration layer — this is the absolute prerequisite.

**Independent Test**: Run `docker compose up` and verify PostGIS functions are available, all services connect via internal network, and data persists across restarts.

**Acceptance Scenarios**:

1. **Given** the Docker Compose environment is started, **When** I run `docker compose up`, **Then** PostgreSQL 16 with PostGIS, hstore, and pgcrypto extensions is available and all service containers start successfully.
2. **Given** the database is running, **When** I execute `SELECT PostGIS_Version()`, **Then** it returns a valid PostGIS version string.
3. **Given** data is inserted into the database, **When** I restart the Docker Compose stack, **Then** the data persists across restarts.

---

### User Story 2 — Import OSM to GIS (Priority: P2)

As a system operator, I want to import OpenStreetMap charging station data into the GIS layer so that station candidates are available for the inventory system.

**Why this priority**: Geospatial data must be ingested before it can be transformed into the inventory model.

**Independent Test**: Run the OSM import script with a known dataset and verify raw geometry data is stored in the GIS staging layer with correct coordinates.

**Acceptance Scenarios**:

1. **Given** an OSM dataset containing charging station POIs, **When** I run the import process, **Then** all geometries are stored as GEOGRAPHY(Point, 4326) in the GIS staging table.
2. **Given** the same OSM dataset has already been imported, **When** I run the import again, **Then** no duplicate records are created.
3. **Given** an OSM import is running, **When** it encounters an error, **Then** the failure is logged and no partial data remains.

---

### User Story 3 — Create inventory schema (Priority: P3)

As a platform engineer, I want to create the EV inventory schema (Partners, Stations, Chargers, Connectors) so that the platform has a structured domain model.

**Why this priority**: The inventory schema is the canonical data store that all other features read from and write to.

**Independent Test**: Run schema migrations and verify all tables exist with correct FKs, nanoid PKs, and typed prefix format.

**Acceptance Scenarios**:

1. **Given** the inventory schema migration has been applied, **When** I inspect the database, **Then** all tables (partners, stations, chargers, connectors) exist with typed nanoid primary keys.
2. **Given** a station record exists, **When** I insert a charger referencing it, **Then** the FK constraint is enforced. **When** I insert a charger referencing a non-existent station, **Then** it is rejected.
3. **Given** a station with chargers exists, **When** I delete the station, **Then** all associated chargers and connectors are cascade-deleted.

---

### User Story 4 — Sync system and nearby SQL function (Priority: P4)

As a platform engineer, I want to build the sync engine and spatial query function so that OSM data flows into the inventory and nearby searches can be executed.

**Why this priority**: The sync pipeline connects ingestion to inventory, and the nearby function is the core spatial query that all driver features depend on.

**Independent Test**: Run the sync pipeline to map OSM staging data into inventory stations. Then execute `find_nearby_stations()` and verify stations sorted by distance.

**Acceptance Scenarios**:

1. **Given** OSM staging data exists, **When** I run the sync pipeline, **Then** stations are created in the inventory with correct geolocation and OSM source tracking.
2. **Given** stations exist in the inventory, **When** I call `find_nearby_stations(lat, lon, radius)`, **Then** results are returned sorted by distance with power tier classification (ultra_fast ≥150kW, fast ≥50kW, medium ≥22kW, slow <22kW) and connector availability counts.
3. **Given** the sync pipeline processes the same OSM data twice, **When** I run it again, **Then** no duplicate stations are created.

---

### User Story 5 — Driver service with health check and nearby endpoint (Priority: P5)

As a driver, I want to query nearby charging stations through a REST API so that applications can discover stations programmatically.

**Why this priority**: The API is the interface that frontends and third parties use to access spatial data.

**Independent Test**: Start the driver service and call GET /health to verify connectivity, then call GET /nearby with coordinates to receive sorted station results.

**Acceptance Scenarios**:

1. **Given** the driver service is running, **When** I call GET /health, **Then** it returns status ok, database connected, and current timestamp.
2. **Given** stations exist in the inventory, **When** I call GET /nearby?lat=X&lon=Y, **Then** I receive stations sorted by distance with power tier, availability, and location data.
3. **Given** no stations exist near a coordinate, **When** I call GET /nearby, **Then** I receive an empty stations array with no errors.

---

### User Story 6 — Driver web app (Priority: P6)

As a driver, I want to see nearby charging stations on a map so that I can visually find and navigate to the closest charging point.

**Why this priority**: The web app is the end-user product that demonstrates the full stack working together.

**Independent Test**: Open the web app, allow location access, and verify station markers appear on the map with distance indicators.

**Acceptance Scenarios**:

1. **Given** the web app is loaded and stations exist nearby, **When** the map renders, **Then** station markers appear at correct geographic locations with power tier badges and distance indicators.
2. **Given** the web app is loaded with no stations nearby, **When** the map renders, **Then** a clear message is displayed that no stations were found nearby.
3. **Given** a station marker is visible, **When** I click on it, **Then** I see station details including connector types, power output, and operational status.

### Edge Cases

- What happens when Docker Compose is started without a persistent volume — is data lost?
- How does the system handle OSM data with missing or malformed geometry?
- What happens when a connector type is not recognized from the lookup table?
- How does the system handle extremely large radius queries (e.g., 500 km)?
- What happens when the driver service starts but the database is not yet ready?
- How does the web app handle a user denying geolocation permission?

## Requirements

### Functional Requirements

- **FR-001**: The system MUST provision a PostgreSQL database with PostGIS, hstore, and pgcrypto extensions on startup.
- **FR-002**: All services MUST connect via an internal Docker network and data MUST persist across container restarts.
- **FR-003**: The system MUST import OSM charging station POIs into a GIS staging table with GEOGRAPHY(Point, 4326) geometry.
- **FR-004**: OSM imports MUST be idempotent — re-running the same dataset MUST NOT create duplicate records.
- **FR-005**: The system MUST enforce a Partner → Station → Charger → Connector hierarchy with typed nanoid primary keys (PAR-, STA-, CHR-, CON-).
- **FR-006**: The system MUST enforce referential integrity — removing a station MUST cascade-delete all child chargers and connectors.
- **FR-007**: The system MUST provide a sync pipeline that maps OSM staging data into inventory stations with source tracking.
- **FR-008**: The system MUST provide a `find_nearby_stations` function that queries only a materialized view and returns results sorted by distance with power tier and availability.
- **FR-009**: The system MUST expose GET /health returning service status, database connectivity, and timestamp.
- **FR-010**: The system MUST expose GET /nearby accepting lat, lon, radius and returning stations sorted by distance.
- **FR-011**: The driver web app MUST render station markers on a map with distance indicators, power tier badges, and availability information.

### Key Entities

- **Partner**: An organization or individual that operates EV charging stations.
- **Station**: A physical geolocated EV charging location belonging to a partner.
- **Charger**: A physical charging device at a station with power specifications.
- **Connector**: A physical plug interface on a charger with type and current mode.
- **Sync Job**: A recorded operation that imports geospatial data from external sources.

## Success Criteria

### Measurable Outcomes

- **SC-001**: Docker Compose starts all services and PostGIS functions are verified available within 30 seconds.
- **SC-002**: An OSM dataset with 50 stations can be imported end-to-end (ingestion → sync → inventory) without errors.
- **SC-003**: Running the same import 3 times produces zero duplicate stations.
- **SC-004**: The inventory schema enforces FK integrity — deleting a station cascades to all children with no orphan records.
- **SC-005**: The `find_nearby_stations` function returns results in under 2 seconds for a typical urban search radius.
- **SC-006**: The GET /nearby API endpoint responds within 150ms for a typical query.
- **SC-007**: The web app renders station markers accurately on a map for any query within a covered region.

## Assumptions

- All services run via Docker Compose for local development.
- OSM is the primary geospatial data source for this sprint.
- The platform is initially deployed for a single geographic region.
- No driver or partner authentication is implemented in this sprint.
- External data imports are initiated manually by system operators.
- Read performance is prioritized over write performance.
