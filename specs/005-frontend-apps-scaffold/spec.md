# Feature Specification: Frontend Apps Scaffold

**Feature Branch**: `005-frontend-apps-scaffold`

**Created**: 2026-06-07

**Status**: Draft

**Input**: User description: "Scaffold three frontend apps — Driver Web, Driver Mobile, Dashboard — with maps, navigation, and real API integration."

## User Scenarios & Testing

### User Story 1 - Driver Web Map Browsing (Priority: P1)

A driver opens the BorneMap website and sees an interactive map with station markers loaded from the real driver-service API. Clicking a marker shows the station name, available charger count, and distance.

**Why this priority**: The web map is the primary user-facing feature of Phase 1. It demonstrates end-to-end integration between frontend and backend.

**Independent Test**: Open the Driver Web app at localhost:5173 — map renders with markers, clicking a marker shows a popup with station info.

**Acceptance Scenarios**:

1. **Given** the Driver Web app is running, **When** a user navigates to the app URL, **Then** a Leaflet map is displayed centered on Tunisia
2. **Given** the map is displayed, **When** station data loads from the API, **Then** markers appear at station locations
3. **Given** a user clicks a station marker, **When** the popup opens, **Then** it shows station name, available charger count, and distance
4. **Given** the API is unreachable, **When** the map loads, **Then** a fallback message is shown (map still renders)

---

### User Story 2 - Driver Mobile Map with Location (Priority: P1)

A driver opens the BorneMap mobile app and sees a native map with their current location (or default coordinates if permission is denied) and station markers from the API.

**Why this priority**: The mobile map is the second primary user-facing touchpoint, reaching drivers on-the-go.

**Independent Test**: Open the Driver Mobile app — MapView renders with markers, location permission prompt appears (or defaults to Tunis coordinates).

**Acceptance Scenarios**:

1. **Given** the Driver Mobile app is running, **When** the map screen loads, **Then** a MapView is displayed with station markers from the API
2. **Given** location permission is granted, **When** the map loads, **Then** the user's location is shown on the map
3. **Given** location permission is denied, **When** the map loads, **Then** the map defaults to Tunis coordinates (36.8065, 10.1815)
4. **Given** a user taps a station marker, **When** the callout appears, **Then** it shows station name and available charger count

---

### User Story 3 - Dashboard Navigation (Priority: P2)

An admin opens the Dashboard web app and sees a left sidebar with four navigation items: Overview, Partners, Stations, Chargers. Clicking a nav item navigates to the corresponding page. The active item is highlighted with the brand color scheme.

**Why this priority**: The Dashboard is an internal tool. Basic navigation scaffolding enables future admin functionality.

**Independent Test**: Open the Dashboard app at localhost:5174 — sidebar renders with four items, clicking each navigates to the correct page, active item is highlighted.

**Acceptance Scenarios**:

1. **Given** the Dashboard app is running, **When** a user opens it, **Then** a left sidebar shows Overview, Partners, Stations, and Chargers
2. **Given** the sidebar is displayed, **When** a user clicks "Partners", **Then** the Partners page is displayed and the nav item shows `#EAF0E6` background with `#007943` text color
3. **Given** a user navigates to Overview, **When** the page loads, **Then** stat cards are displayed (total partners, total stations, total chargers)

### Edge Cases

- What happens when the driver-service API is unreachable? → Map still renders, error message shown for markers
- What happens when location permission is denied on mobile? → Default coords used, no crash
- What happens when an invalid route is accessed in Dashboard? → 404 page or redirect to Overview
- What happens on narrow screens on Dashboard? → Sidebar collapses or overlays

## Requirements

### Functional Requirements

- **FR-001**: Driver Web MUST display a Leaflet map centered on Tunisia (34.0, 9.0) with zoom level 7
- **FR-002**: Driver Web MUST fetch stations from `GET /api/v1/stations/nearby?lat=...&lng=...&radius_km=50`
- **FR-003**: Driver Web MUST display station markers with a popup showing station name, available charger count, calculated distance
- **FR-004**: Driver Web MUST proxy `/api/v1` requests to the driver-service backend
- **FR-005**: Driver Mobile MUST display a MapView with station markers fetched from the driver-service API
- **FR-006**: Driver Mobile MUST request location permission on first launch
- **FR-007**: Driver Mobile MUST fall back to default coordinates (36.8065, 10.1815) when location permission is denied
- **FR-008**: Driver Mobile MUST show a callout with station name and charger count when a marker is tapped
- **FR-009**: Dashboard MUST display a left sidebar with navigation links: Overview, Partners, Stations, Chargers
- **FR-010**: Dashboard MUST highlight the active nav item with `#EAF0E6` background and `#007943` text
- **FR-011**: Dashboard MUST display an Overview page with stat cards for total partners, stations, and chargers
- **FR-012**: Dashboard MUST display placeholder pages for Partners, Stations, and Chargers
- **FR-013**: All three apps MUST pass their respective CI workflows

### Key Entities

- **Station**: A charging station location with name, coordinates (lat/lng), address, and available charger count. Returned by the driver-service API.
- **Partner**: An organization that owns stations. Displayed in Dashboard overview stats.
- **Charger**: An individual charging unit at a station. Count shown in marker popups.

## Success Criteria

### Measurable Outcomes

- **SC-001**: Driver Web loads map in under 3 seconds on a broadband connection
- **SC-002**: Station markers appear within 2 seconds of API response
- **SC-003**: Dashboard sidebar navigation responds to clicks in under 100ms
- **SC-004**: Driver Mobile app starts without crashes on both iOS simulator and Android emulator

## Assumptions

- Driver-service is running and accessible at the configured proxy target (localhost:3001 for dev)
- API returns valid GeoJSON-style or JSON coordinates
- Expo SDK 54 and react-native-maps 1.18.0 are compatible with current toolchain
- Tailwind CSS v3 is the styling framework used across all web apps
- Marker popup design is minimal (no custom component library needed)
- Authentication is not required for any of these screens (deferred to Phase 2)
