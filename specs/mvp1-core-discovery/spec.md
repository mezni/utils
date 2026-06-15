# Feature Specification: MVP-1 Core Geospatial Discovery

**Feature Branch**: `mvp1-core-discovery`

**Created**: 2026-06-14

**Status**: Draft

**Input**: User description: "Build BorneMap MVP-1 — a lean geospatial system for EV charging station discovery in Tunisia. Core pipeline: OSM → PostGIS → Rust Driver Service → Mobile + Web Map."

## User Scenarios & Testing

### User Story 1 — Nearby Station Discovery on Map (Priority: P1)

A driver opens the BorneMap mobile or web app, sees Tunisia on the map, and views nearby charging stations within a configurable radius.

**Why this priority**: This is the core value proposition — discovering charging stations geographically. Without this, there is no product.

**Independent Test**: Can be fully tested by loading OSM data, running the driver-service, opening the mobile or web app, and verifying station markers appear on the map.

**Acceptance Scenarios**:

1. **Given** the system is running with imported OSM Tunisia station data, **When** a user opens the mobile or web app, **Then** the map displays station markers at correct geographic coordinates.
2. **Given** a user is viewing the map, **When** they pan to a new area, **Then** new stations within the viewport are fetched via API with a 300ms debounced request.
3. **Given** a station has `availability = 'AVAILABLE'` in the database, **When** the API returns nearby stations, **Then** the station appears with an available indicator on the map.
4. **Given** a station has `availability = 'OUT_OF_SERVICE'`, **When** the API returns nearby stations, **Then** the station does not appear in results (GIS filters it out).

---

### User Story 2 — Station Data via API (Priority: P2)

A developer or app queries the API and receives station details including name, address, distance, and charger composition.

**Why this priority**: Essential for decision-making — a driver needs to know if a station has compatible chargers before navigating there.

**Independent Test**: Can be fully tested by calling the API endpoint directly with curl and verifying the response contains charger details.

**Acceptance Scenarios**:

1. **Given** a station has 2 chargers (CCS2 150kW, Type2 22kW) in `inventory.chargers`, **When** the API returns nearby stations, **Then** the response JSONB array contains both chargers with correct plug types and power ratings.
2. **Given** a station has no chargers configured, **When** the API returns that station, **Then** the response contains an empty `available_chargers` array.

---

### User Story 3 — API Health Check and Error Handling (Priority: P3)

A developer deploys the system and verifies the service is running and responding correctly, including graceful error handling for invalid inputs.

**Why this priority**: Operational necessity for deployment and debugging.

**Independent Test**: Can be tested with curl against the health endpoint and with malformed coordinates.

**Acceptance Scenarios**:

1. **Given** the driver-service is running, **When** a GET request is sent to `/api/v1/health`, **Then** the response returns 200 with status `ok`.
2. **Given** a request with coordinates outside Tunisia bounds, **When** the API is called, **Then** the response returns 400 with an appropriate error message.
3. **Given** a request with missing query parameters, **When** the API is called, **Then** the response returns 422.

### Edge Cases

- What happens when the PostGIS database is unreachable? → Service returns 503.
- What happens when OSM data is empty? → API returns empty `[]` station list, map shows no markers.
- What happens when coordinates exactly on Tunisia boundary? → Boundary check accepts inclusive ranges (7.0/12.0/30.0/38.0).
- What happens when search_radius_meters is negative? → API rejects with 422.

## Requirements

### Functional Requirements

- **FR-001**: System MUST import EV charging station data from OpenStreetMap for Tunisia.
- **FR-002**: System MUST store station data in a PostGIS-enabled PostgreSQL database.
- **FR-003**: System MUST provide a REST API endpoint for nearby station queries.
- **FR-004**: System MUST use `ST_DWithin` with geography cast for proximity queries.
- **FR-005**: System MUST return station details including aggregated charger information as JSONB.
- **FR-006**: System MUST filter out unavailable (`OUT_OF_SERVICE`) stations from results.
- **FR-007**: System MUST validate driver coordinates are within Tunisia bounds.
- **FR-008**: Mobile and web maps MUST render station markers from API response.
- **FR-009**: Client apps MUST debounce API calls by 300ms during map panning.
- **FR-010**: Mobile markers MUST use `tracksViewChanges = false` for performance.
- **FR-012**: Web driver MUST use Leaflet with react-leaflet for map rendering.
- **FR-011**: System MUST replicate `inventory.stations` writes to `gis.osm_stations` via trigger.

### Key Entities

- **Partner**: Organization that owns charging stations. Fields: id, name, type (BUSINESS/PRIVATE), email, phone, verified status.
- **Station**: Physical charging location. Fields: id, partner_id, name, address, email, coordinates (lat/lon), availability, verified, is_live status.
- **Charger**: Individual charging hardware at a station. Fields: id, station_id, identifier_code, plug_type, max_power_kw, status.
- **Plug Type**: Standard charging connector type (ccs2, type2, chademo). Immutable configuration reference.
- **OSM Station**: GIS-cached station with geometry, synchronized from inventory via trigger.

## Data Flow

```
OSM Overpass API → import-tunisia-osm.sh → gis.osm_stations (direct insert, source='OSM_IMPORT')
                                                    ↓
                                        gis.get_nearby_stations()
                                                    ↓
                                        driver-service /api/v1/stations/nearby
                                                    ↓
                              ┌─────────────────────┴──────────────────┐
                              ▼                                        ▼
                      mobile-app (Expo)                         web-driver (Leaflet)
                      react-native-maps markers                  react-leaflet markers
```

## Success Criteria

### Measurable Outcomes

- **SC-001**: Nearby station query returns results in under 150ms with spatial index.
- **SC-002**: Mobile app renders 100+ station markers without jank (60 FPS).
- **SC-003**: API validates coordinates and rejects out-of-bounds requests with clear error.
- **SC-004**: Docker stack starts all services with a single `docker compose up` command.
- **SC-005**: OSM import script loads Tunisia EV stations into the database in under 5 minutes.

## Assumptions

- User has Docker and Docker Compose installed.
- Mobile development uses Expo Go for testing.
- Web driver uses Vite + React for dev server.
- Tunisia EV station data is available via OSM Overpass API.
- Network latency between clients and driver-service is <100ms for local dev.
- No authentication is required for MVP-1 — all requests use mock identity.
