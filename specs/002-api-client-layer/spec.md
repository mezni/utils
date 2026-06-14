# Feature Specification: API Client Layer

**Feature Branch**: `002-api-client-layer`

**Created**: 2026-06-13

**Status**: Draft

**Input**: User description: "Sprint 2 from MVP-1 Discovery — API Client Layer: Single source of truth for frontend API. Create @bm/api-client package with getStations(), getStationById(), getNearbyStations(). Fully typed responses shared between web and mobile. No fetch outside this package."

## User Scenarios & Testing

### User Story 1 - Frontend queries all stations via shared client (Priority: P1)

As a frontend developer building the web or mobile map view, I want to call a single `getStations()` function from the shared API client so that I can load and display all EV charging stations without writing raw requests.

**Why this priority**: This is the foundational capability — without it, neither web nor mobile app can display station data. Every other feature depends on this layer.

**Independent Test**: Can be fully tested by calling `getStations()` and verifying the returned data matches the backend response shape. Delivers the ability for any frontend to query station data.

**Acceptance Scenarios**:

1. **Given** the driver-service backend is running with stations in the database, **When** a frontend app calls `getStations()`, **Then** it receives a typed array of station objects matching the expected response shape.
2. **Given** the driver-service is unavailable, **When** a frontend app calls `getStations()`, **Then** the client throws a typed error that can be caught and handled by the caller.

---

### User Story 2 - Frontend queries station details by ID (Priority: P1)

As a frontend developer, I want to call `getStationById(id)` from the shared API client so that I can fetch details for a specific station when a user taps a marker.

**Why this priority**: Station detail display is a core user-facing feature. The API client must support fetching individual records with the same type guarantees.

**Independent Test**: Can be tested by calling `getStationById` with a known station ID and verifying the returned single station object. Delivers the complete station detail flow.

**Acceptance Scenarios**:

1. **Given** a station exists with a known ID, **When** a frontend calls `getStationById(id)`, **Then** it returns the correct typed station object.
2. **Given** an invalid or non-existent station ID, **When** a frontend calls `getStationById(id)`, **Then** the client returns a typed 404 error.

---

### User Story 3 - Frontend queries nearby stations by coordinates (Priority: P1)

As a frontend developer, I want to call `getNearbyStations(lat, lng, radius)` from the shared API client so that I can fetch stations near the current map viewport.

**Why this priority**: The nearby query is the core geospatial interaction of MVP-1. The API client must support parameterized queries with proper types.

**Independent Test**: Can be tested by calling `getNearbyStations` with valid coordinates and radius, verifying the returned stations are within range. Delivers the map-based discovery experience.

**Acceptance Scenarios**:

1. **Given** stations exist within a given radius of coordinates, **When** a frontend calls `getNearbyStations(lat, lng, radius)`, **Then** it returns an array of nearby stations sorted by distance.
2. **Given** no stations exist within the given radius, **When** a frontend calls `getNearbyStations(lat, lng, radius)`, **Then** it returns an empty array.

---

### Edge Cases

- What happens when the API client is called with invalid parameters (e.g., out-of-range latitude)?
- How does the system handle network timeouts or connectivity loss?
- What happens when the backend returns an unexpected response format?
- How does the API client behave when the base URL is misconfigured?
- What happens when multiple rapid calls are made (e.g., during map panning)?

## Requirements

### Functional Requirements

- **FR-001**: The API client MUST expose a `getStations()` function that fetches all stations from `GET /api/v1/stations` and returns a typed array of station objects.
- **FR-002**: The API client MUST expose a `getStationById(id: string)` function that fetches a single station from `GET /api/v1/stations/{id}` and returns a typed station object.
- **FR-003**: The API client MUST expose a `getNearbyStations(lat: number, lng: number, radius: number)` function (radius in meters) that fetches nearby stations from `GET /api/v1/stations/nearby?lat&lng&radius` and returns a typed array of station objects sorted by distance.
- **FR-004**: All API client functions MUST return typed responses using shared model types from `@bm/types`.
- **FR-005**: The API client MUST throw typed errors for network failures, non-2xx responses, and unexpected response shapes.
- **FR-006**: The API client MUST be usable from both `mobile-driver` (React Native) and `web-driver` (React/Leaflet) without modification.
- **FR-007**: The API client MUST NOT use `fetch` or `axios` directly — it MUST rely on the underlying platform HTTP capabilities abstracted through a transport layer.
- **FR-008**: The API client MUST NOT be used outside of `@bm/api-client` — no frontend app may make raw HTTP requests.
- **FR-009**: The API client MUST accept a configurable base URL at initialization time.

### Key Entities

- **Station**: Represents an EV charging station with properties: id, name, status, latitude, longitude, location (geospatial), distance (computed). Shared type across backend and frontend.
- **ApiClient**: The configured client instance exposing typed methods for all station endpoints. Single instance shared within each app.

## Success Criteria

### Measurable Outcomes

- **SC-001**: A frontend developer can integrate the API client into both web and mobile apps with fewer than 10 lines of configuration code.
- **SC-002**: All three API functions (`getStations`, `getStationById`, `getNearbyStations`) return correctly typed responses that match the backend response shape without manual type assertions.
- **SC-003**: The same API client code compiles and runs in both web (React/Leaflet) and mobile (React Native/Expo) environments without platform-specific modifications.
- **SC-004**: A failing API call (network error, 404, 500) produces a typed error object that the calling code can match against to show appropriate user-facing messages.
- **SC-005**: No raw `fetch` or `axios` calls exist in any frontend application code — all HTTP traffic goes through the API client.

## Assumptions

- The driver-service backend is already running and exposing `GET /api/v1/stations`, `GET /api/v1/stations/{id}`, and `GET /api/v1/stations/nearby` endpoints.
- Shared types for Station are already defined in `@bm/types` or will be created as part of this sprint.
- The API client will be published as a workspace package under `source/front/packages/@bm/api-client`.
- The base URL for the driver-service will be provided via environment variable or app configuration.
- Both mobile and web apps use a module bundler that can resolve the shared workspace package.
