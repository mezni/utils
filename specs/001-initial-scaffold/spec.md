# Feature Specification: BorneMap Platform Scaffold

**Feature Branch**: `002-initial-scaffold`

**Created**: 2026-05-27

**Status**: Draft

**Input**: User description: "Scaffold the full BorneMap platform -- directory architecture, Docker stack with PostGIS, GitHub Actions CI/CD, Rust backend workspace with mock api-service, React Native mobile driver app with map view and station discovery, shared data dictionaries, and Makefile operational tooling."

## Clarifications

### Session 2026-05-27

- Q: When the API is unreachable, returns an error, or returns an empty station list, what should the mobile app display? → A: Show a dedicated error screen with a "Retry" button and descriptive message; empty state also shows a friendly illustration.
- Q: Can users discover stations outside the default Tunis region by moving the map, or is discovery fixed to Tunis? → A: Users can pan/zoom freely; the app fetches stations based on the visible map center whenever map movement settles.
- Q: How should the mobile app refresh station data and availability status? → A: Pull-to-refresh gesture on the map view, plus automatic refresh every 30 seconds while the app is in the foreground.
- Q: Which actions should be available from the station detail card? → A: View station details and launch navigation to the station in the device's default maps app.
- Q: Should the MVP include a search bar or location input for users to type an address or place name? → A: Yes; include a basic search bar that accepts place names and centers the map on the result, using the device's geocoding capability.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Driver Discovers Nearby Charging Stations (Priority: P1)

A driver opens the BorneMap mobile app and sees charging stations near the city center of Tunis on an interactive map. They can tap any station to view details including available chargers, plug types, and power output. The map defaults to the Tunis region and displays station availability through visual indicators.

**Why this priority**: This is the core value proposition of the platform -- enabling drivers to find and evaluate charging stations. Without this flow, the product has no purpose.

**Independent Test**: Can be fully tested by launching the mobile app with mock data and verifying that stations appear on the map, markers show correct availability colors, and tapping a marker displays the station detail card with charger information.

**Acceptance Scenarios**:

1. **Given** the mobile app is launched, **When** the map screen loads, **Then** at least two charging stations appear as markers centered on Tunis, Tunisia.
2. **Given** a station marker is displayed on the map, **When** the driver taps the marker, **Then** a station detail card appears showing station name, provider name, status, and a list of chargers with plug type and power output.
3. **Given** the station detail card is visible, **When** a charger is listed, **Then** each charger entry shows its plug type (e.g., CCS2), power output in kW, and current availability status.

---

### User Story 2 - Filter Stations by Availability (Priority: P2)

A driver wants to see only stations that currently have available chargers. They tap the "Available" filter button above the map, and the map updates to show only stations with at least one available charger.

**Why this priority**: Improves the core experience by reducing time spent evaluating occupied stations. Delivers immediate time savings for drivers.

**Independent Test**: Can be tested by applying the "Available" filter and verifying that only stations with status "Available" remain visible on the map and in the station list.

**Acceptance Scenarios**:

1. **Given** the map screen is loaded with multiple stations, **When** the driver taps the "Available" filter button, **Then** only stations with status "Available" remain visible on the map.
2. **Given** the "Available" filter is active, **When** the driver taps "All", **Then** all stations reappear on the map.

---

### User Story 3 - Backend API Returns Consistent Station Data (Priority: P2)

The `api-service` backend exposes a `/stations/nearby` endpoint that returns a list of charging stations with their chargers. The response follows a defined JSON contract with consistent identifier formatting and timestamp conventions.

**Why this priority**: The backend API is the foundation for all mobile and web clients. A stable, well-defined contract ensures all consumers receive predictable data.

**Independent Test**: Can be tested by sending a GET request to `/api/v1/stations/nearby` and validating the response matches the StationHub JSON schema including identifier format (XXX-nanouuid), coordinate precision, and timestamp format.

**Acceptance Scenarios**:

1. **Given** the `api-service` is running, **When** a GET request is sent to `/api/v1/stations/nearby`, **Then** the response is a JSON array of station objects.
2. **Given** a station object in the response, **When** its `id` field is inspected, **Then** it matches the pattern `^[a-z]{3}-[a-f0-9]{8}$`.
3. **Given** a station object in the response, **When** its `chargers` array is inspected, **Then** each charger contains `id`, `plug_type`, `power_output`, and `status` fields.

---

### Edge Cases

- **API failure or unreachable**: The mobile app displays a dedicated error screen with a descriptive message and a "Retry" button. The map remains visible behind the overlay with any previously loaded station data.
- **Empty station list (no results)**: The app shows a friendly illustration and message indicating no stations found in the area, with a suggestion to try again later.
- **Network connectivity loss**: The app detects offline state and displays a non-blocking banner at the top of the screen indicating no connection. Existing station data (if any) remains visible on the map.
- **Stations with identical coordinates**: Overlapping markers are handled by allowing users to tap to cycle through them, or by clustering if the map zoom level warrants it.
- **Station with no chargers listed**: The station card displays a message indicating charger details are unavailable, but the station itself remains visible on the map.
- **Slow API response**: A loading spinner or skeleton UI is shown on initial load with a timeout threshold; if exceeded, the error screen with retry replaces the loading state.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST display an interactive map centered on Tunis, Tunisia by default. Users can pan and zoom freely, and the system MUST re-fetch nearby stations based on the new visible map center after map movement settles.
- **FR-002**: System MUST show station markers on the map with color-coded availability status.
- **FR-003**: Users MUST be able to tap a station marker to view station details including name, provider, status, and charger list. The station detail card MUST include a button to launch navigation to the station in the device's default maps app.
- **FR-004**: System MUST provide a filter to show only stations with available chargers.
- **FR-005**: System MUST expose a `GET /api/v1/stations/nearby` endpoint returning stations with their chargers.
- **FR-006**: System MUST return station identifiers following the `XXX-nanouuid` format.
- **FR-007**: System MUST seed at least two mock stations with Tunisian geographic coordinates for development and testing.
- **FR-008**: System MUST support pull-to-refresh and automatic 30-second foreground refresh of station data.
- **FR-009**: System MUST support running the backend and database via Docker Compose for local development.
- **FR-010**: System MUST run automated backend tests (format check, compilation, unit tests) on every push and pull request.
- **FR-011**: System MUST run frontend build verification on every push and pull request.
- **FR-012**: System MUST include a search bar that accepts place names and centers the map on the geocoded result, triggering a nearby station re-fetch.

### Key Entities *(include if feature involves data)*

- **StationHub**: A physical EV charging station location. Key attributes include geographic coordinates (latitude/longitude), operational status, provider information, and a list of chargers. Identified by a nanouuid with prefix `stn-`.
- **Charger**: An individual charging unit at a station. Key attributes include plug type (e.g., CCS2), power output in kW, and current status. Identified by a nanouuid with prefix `chg-`.
- **Provider**: The organization operating one or more stations. Key attributes include name. Identified by a nanouuid with prefix `prv-`.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A developer can start the full local development stack (database + API) with a single command in under 2 minutes.
- **SC-002**: The mock API endpoint returns the StationHub JSON payload within 100ms on local development hardware.
- **SC-003**: The mobile app displays an interactive map with station markers within 5 seconds of launch on a standard device.
- **SC-004**: The CI pipeline completes (format check, compilation, tests, frontend build) in under 5 minutes.
- **SC-005**: Station identifiers in all API responses consistently match the `^[a-z]{3}-[a-f0-9]{8}$` pattern, verified by automated tests.

## Assumptions

- Development will occur on Linux or macOS machines with Docker and Node.js installed.
- The initial release targets the Tunis, Tunisia metro area only.
- Mock data is sufficient for MVP development; live data integration is a future concern.
- The mobile app will be developed and tested using Expo Go on physical devices or emulators.
- Station availability status is provided by the mock data layer and will later be driven by real-time station telemetry.
- The authentication and user identity layer (`auth-service`) is out of scope for this initial scaffold; all endpoints start as unauthenticated for development velocity.
