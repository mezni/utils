# Feature Specification: Mobile Driver App (Core UX)

**Feature Branch**: `006-mobile-driver-app`

**Created**: 2026-06-11

**Status**: Draft

**Input**: User description: "read from docs/mvp sprint 1.5"

## User Scenarios & Testing

### User Story 1 — Map Screen with Station Markers (Priority: P1)

A driver opens the Borne mobile app and sees a full-screen map centered on their current location. Station markers appear on the map for nearby charging stations. As the driver pans or zooms, the map fetches fresh stations for the visible area and updates the markers.

**Why this priority**: The map is the primary screen of the entire MVP. Without it, the driver has no discovery mechanism. All downstream interactions (station details, navigation) depend on the map being functional.

**Independent Test**: Open the app in a simulator with a known location (Tunis). Verify the map renders full-screen with station markers at expected coordinates. Pan to a new area and verify markers update within 5 seconds.

**Acceptance Scenarios**:

1. **Given** the app is launched, **When** the map screen loads, **Then** a full-screen map is displayed centered on the user's approximate location
2. **Given** the map is visible, **When** the nearby search completes, **Then** station markers appear on the map at the correct coordinates
3. **Given** station markers are displayed, **When** the user pans to a new area, **Then** new markers load for the visible region
4. **Given** no stations are found nearby, **When** the search completes, **Then** an empty state message is shown on the map ("No stations nearby")

---

### User Story 2 — Station Details Bottom Sheet (Priority: P1)

A driver taps a station marker on the map. A bottom sheet slides up with the station name, distance from the user, a list of available chargers with their status (available/occupied/offline), and the connector type for each charger.

**Why this priority**: The station detail sheet is the core information delivery mechanism. The driver needs to see station details to decide where to go. Without it, markers are meaningless.

**Independent Test**: Tap a station marker on the map. Verify a bottom sheet animates up from the bottom of the screen showing station name, distance, and at least one charger entry with status. Tap a different marker and verify the sheet content updates.

**Acceptance Scenarios**:

1. **Given** a station marker is visible on the map, **When** the driver taps it, **Then** a bottom sheet slides up with the station name and distance
2. **Given** the bottom sheet is open, **When** the station detail API returns, **Then** a list of chargers is displayed with each charger's connector type and status (available/occupied/offline)
3. **Given** the bottom sheet is open with content, **When** the driver taps a different marker, **Then** the sheet content updates to the new station
4. **Given** the bottom sheet is open, **When** the driver swipes down, **Then** the sheet dismisses and the map is fully visible again

---

### User Story 3 — Skeleton Loading & Error States (Priority: P1)

While the map or station details are loading, the driver sees animated skeleton placeholders instead of blank screens. If a network error occurs, a friendly error state with a retry button is shown. The driver never sees a blank, frozen, or broken screen.

**Why this priority**: The MVP UX rules mandate skeleton-first loading on every screen. No blank states are permitted. This is a hard requirement for the stabilization gate.

**Independent Test**: Load the app with the network disabled. Verify the skeleton map placeholder appears during loading and an error state with "Retry" appears after failure. Re-enable the network, tap Retry, and verify the map loads successfully.

**Acceptance Scenarios**:

1. **Given** the app is loading the map for the first time, **When** the nearby search is in progress, **Then** a full-screen map skeleton with shimmer animation is displayed
2. **Given** the bottom sheet is opening, **When** the station detail API call is in progress, **Then** a skeleton placeholder is shown inside the sheet
3. **Given** a network request fails (map or station details), **When** the error is caught, **Then** an error state with a descriptive message and a "Retry" button is shown
4. **Given** the driver taps "Retry" after an error, **When** the request is re-sent, **Then** the loading skeleton is shown again followed by the content on success

---

### User Story 4 — Interaction Event Tracking (Priority: P2)

As the driver uses the app — opening the map, searching for nearby stations, tapping markers, viewing station details — each interaction generates a clickstream event that is sent to the Clickstream Service for analytics.

**Why this priority**: Event tracking provides critical product usage data. Without it, the team cannot measure adoption, identify drop-off points, or validate the core UX loop.

**Independent Test**: Open the app and perform a series of actions (pan map, tap marker, view sheet). Verify that corresponding events (map_open, map_pan, station_click, station_view) appear in the analytics database within a few seconds.

**Acceptance Scenarios**:

1. **Given** the app launches, **When** the map screen renders, **Then** a `map_open` event is sent to the Clickstream Service
2. **Given** the user pans or zooms the map, **When** the map region changes, **Then** a `map_pan` or `map_zoom` event is sent
3. **Given** the user taps a station marker, **When** the marker is selected, **Then** a `station_click` event is sent with the station ID
4. **Given** the bottom sheet opens for a station, **When** the detail is displayed, **Then** a `station_view` event is sent with the station ID
5. **Given** the user performs a nearby search (pan or zoom), **When** the search completes, **Then** a `nearby_search` event is sent with the search parameters

---

### Edge Cases

- What happens when GPS is unavailable or denied? → The map centers on a default location (Tunis city center) and shows an empty state explaining that GPS is needed for accurate nearby search
- What happens when the Clickstream Service is unreachable? → Event sending fails silently; the UX loop is never blocked
- What happens when the Driver Service is unreachable? → The skeleton loading times out after 10 seconds; an error state with "Retry" is shown
- What happens when the user taps a marker before the nearby search completes? → The marker tap is queued; the bottom sheet opens once the station detail API responds
- What happens when the user rapidly taps multiple markers? → Each tap triggers a new station detail request; the bottom sheet updates to the most recent station
- What happens when the bottom sheet content is taller than the screen? → The sheet content scrolls independently; the sheet handle remains visible for dismissal

## Requirements

### Functional Requirements

- **FR-001**: System MUST render a full-screen interactive map centered on the user's approximate location on app launch
- **FR-002**: System MUST fetch nearby stations from the Driver Service (`GET /api/v1/stations/nearby`) and display them as markers on the map
- **FR-003**: System MUST re-fetch nearby stations when the user pans or zooms to a new map region
- **FR-004**: System MUST display a station detail bottom sheet when a marker is tapped, showing station name, distance, charger list with connector type and status
- **FR-005**: System MUST fetch station details from the Driver Service (`GET /api/v1/stations/{id}`) when a marker is tapped
- **FR-006**: System MUST show a full-screen map skeleton with shimmer animation while the initial nearby search is loading
- **FR-007**: System MUST show a skeleton placeholder in the bottom sheet while station details are loading
- **FR-008**: System MUST show an error state with a descriptive message and "Retry" button when a network request fails
- **FR-009**: System MUST show an empty state message when the nearby search returns zero stations
- **FR-010**: System MUST send clickstream events for `map_open`, `map_pan`, `map_zoom`, `station_click`, `station_view`, and `nearby_search` to the Clickstream Service (`POST /api/v1/events`)
- **FR-011**: Clickstream event sending MUST NOT block or delay the user interface
- **FR-012**: System MUST gracefully handle GPS denial by showing a default map location and an explanatory message
- **FR-013**: System MUST gracefully handle Driver Service unavailability by showing an error state with 10-second timeout

### Key Entities

- **Station Marker**: A map annotation representing a charging station at its geographic coordinates. Displays a pin/icon on the map. Tapping opens the station detail bottom sheet.
- **Station Detail**: Information about a specific station including name, distance from user, and a list of chargers. Retrieved from the Driver Service and displayed in the bottom sheet.
- **Charger Entry**: A row within the station detail showing connector type (e.g., Type 2, CCS, CHAdeMO), power rating, and current status (available, occupied, offline).
- **Skeleton Placeholder**: An animated loading shape sourced from the design system's Skeleton component. Variants: map skeleton (full-screen), list skeleton (bottom sheet rows).
- **Clickstream Event**: A structured interaction record sent to the Clickstream Service. Includes event type, timestamp, station ID (where applicable), and map region (where applicable).

## Success Criteria

### Measurable Outcomes

- **SC-001**: Map loads and displays station markers within 3 seconds of app launch (on a mid-range device with average network)
- **SC-002**: Station detail bottom sheet opens within 1 second of marker tap (including API fetch time)
- **SC-003**: No blank or frozen screens during loading — skeleton placeholders are shown for every loading state (100% coverage)
- **SC-004**: All map interactions (pan, zoom, marker tap, sheet view) generate corresponding clickstream events with correct event type and station IDs
- **SC-005**: User can complete the core discovery loop (open app → see map → find station → view details) without any error states under normal network conditions

## Assumptions

- The Expo SDK 54 project will be initialized in the `source/front/` directory, which already contains the design system package at `source/front/packages/design-system/`
- The map library is `react-native-maps` (Apple Maps on iOS, Google Maps on Android)
- The routing library is Expo Router (standard for Expo SDK 54)
- The Driver Service is available at `http://<host>:8080` and the Clickstream Service at `http://<host>:8082` (configurable via environment variables or a config module)
- The design system components (Skeleton, EmptyState, ErrorState, BottomSheet, Button) are already implemented and importable from `@borne/design-system`
- GPS permission is requested on first launch via the standard Expo Location API
- The Clickstream Service uses fire-and-forget HTTP — events may be lost if the service is down, but the UX is never blocked
- Station markers use the default `react-native-maps` marker annotation (custom marker icons are future work)
