# Feature Specification: Web Driver Client

**Feature Branch**: `004-web-driver-client`

**Created**: 2026-06-18

**Status**: Draft

**Input**: Sprint 1.4 — Web Driver Client Setup & Map Visualization under `source/apps/web-driver/`

## User Scenarios & Testing

### User Story 1 - Interactive Map with Station Markers (Priority: P1)

A driver opens the web app and sees a full-screen Leaflet map centered on their location (or defaulting to Tunis). Charging stations within the viewport are fetched from `/api/v1/nearby` and displayed as custom charging pin markers. The map is constrained to the Tunisia bounding box.

**Why this priority**: The interactive map is the primary interface for the web driver client. Without it, the application delivers no value. All downstream features (station details, navigation, filtering) depend on this foundation.

**Independent Test**: A user opens the web app, the map renders at their location (or Tunis default), and station markers appear as charging pin icons. Panning outside Tunisia is blocked by boundary constraints.

**Acceptance Scenarios**:

1. **Given** the web app is loaded, **When** the browser has geolocation permission, **Then** the map centers on the driver's current location and fetches nearby stations within the fixed 10km radius
2. **Given** the web app is loaded without geolocation permission, **When** the driver denies location access, **Then** the map defaults to Tunis (36.8, 10.18) at zoom level 12
3. **Given** station data is returned from the API, **When** markers are rendered, **Then** custom charging pin vector icons are displayed at each station's coordinates
4. **Given** the driver pans the map, **When** the viewport center moves, **Then** Tunisia boundary constraints prevent panning outside approx 30-38°N, 7-12°E

---

### User Story 2 - Viewport Debouncing and Station Refresh (Priority: P2)

Viewport changes (pan/zoom) are debounced by 300ms before triggering a new `/api/v1/nearby` query. This prevents excessive API calls during rapid map interaction while ensuring stations are fetched when the user settles on a new area.

**Why this priority**: Without debouncing, every pan/zoom pixel change would fire API calls, overwhelming both the network and the backend. This directly supports the 100-1000 concurrent user target from Sprint 1.3.

**Independent Test**: A driver pans the map rapidly across Tunisia. No API calls are made during the pan gesture. After panning stops for 300ms, a single API call is made for the new viewport center.

**Acceptance Scenarios**:

1. **Given** the driver is panning the map, **When** viewport coordinates change continuously, **Then** no API calls are made until the viewport remains stable for 300ms
2. **Given** the viewport has been stable for 300ms, **When** the debounce timer fires, **Then** a single `/api/v1/nearby` call is made with the new center coordinates
3. **Given** a debounced API call is in flight, **When** the user starts panning again, **Then** the in-flight request is cancelled (no stale data applied)

---

### User Story 3 - Loading, Error, and Empty States with Shimmer (Priority: P2)

During all API interactions, the driver sees polished UX states: shimmer/loading placeholders while fetching, a visual error boundary with retry on failure, and an empty-state message when no stations are found nearby.

**Why this priority**: The constitution mandates loading/error/empty states for every UI component. Without these, users would see a blank or frozen map during failures — a critical UX gap. Shimmer components specifically prevent layout pop.

**Independent Test**: Each state can be observed by simulation: slow network (shimmer), API failure (error boundary with retry), and remote area with no stations (empty state with suggestion).

**Acceptance Scenarios**:

1. **Given** the map is loading nearby stations, **When** the API call is in progress, **Then** a shimmer placeholder is displayed over the map area (not a spinner) to prevent layout pop
2. **Given** the API call fails (timeout, network error, or non-2xx after 10s), **When** the error is detected, **Then** a visual error boundary component is shown with a user-friendly message and retry button
3. **Given** the API returns zero stations, **When** the empty response is received, **Then** a message is displayed indicating no stations nearby with an option to expand scope
4. **Given** the error boundary is displayed, **When** the driver clicks retry, **Then** the API call is re-attempted (max 3 retries per manual attempt)

---

### User Story 4 - Macro-Zoom Overlay (Priority: P3)

When the user zooms out beyond a threshold where station markers would overlap or become indistinguishable, a full-viewport overlay appears indicating the map is zoomed out too far. This prevents performance degradation and visual clutter from dense marker rendering.

**Why this priority**: While not critical for MVP launch, this significantly improves UX at low zoom levels and prevents the browser tab from becoming unresponsive due to excessive marker rendering.

**Independent Test**: The user zooms the map out to a continental or country-wide view. An overlay appears covering the map with a message to zoom in for station details. Markers are hidden beneath the overlay.

**Acceptance Scenarios**:

1. **Given** the map is zoomed out beyond zoom level 8, **When** the threshold is crossed, **Then** a macro-zoom overlay covers the map with a message: "Zoom in to see charging stations"
2. **Given** the macro-zoom overlay is displayed, **When** the user zooms back in to zoom level 8 or above, **Then** the overlay is removed and station markers are shown
3. **Given** the macro-zoom overlay is active, **When** the user attempts to interact with markers beneath it, **Then** interactions are blocked (markers are hidden/not rendered)

---

### User Story 5 - Station Detail Drawer (Priority: P3)

When a driver clicks a station marker, a premium sliding drawer panel opens from the bottom or side of the screen displaying detailed station information: name, distance, partner, address, and available charger types.

**Why this priority**: This transforms the map from a simple visualization into an actionable tool. Drivers need station details to make informed decisions about where to charge.

**Independent Test**: The driver clicks any station marker on the map. A sliding drawer animates into view with the station's full details. The drawer can be dismissed by clicking outside or pressing a close button.

**Acceptance Scenarios**:

1. **Given** station markers are displayed on the map, **When** the driver clicks any marker, **Then** a sliding drawer animates open showing station name, distance, partner name, and address
2. **Given** the station drawer is open, **When** the driver clicks outside the drawer or presses the close button, **Then** the drawer slides closed with a smooth animation
3. **Given** the station drawer is open, **When** the driver pans the map, **Then** the drawer remains open and stays anchored to its station (or closes if the marker leaves the viewport)

---

### Edge Cases

- What happens when the browser tab is backgrounded and then refocused? The map should not re-fetch aggressively — stale data within 60 seconds is reused.
- What happens when the device is offline and the map tiles are not cached? A graceful offline error state should appear (tiles may show grey area), with a retry button.
- What happens on ultra-wide or ultra-narrow screens? The map should be fully responsive; the station drawer should adapt to side panel (wide) or bottom sheet (narrow) layout.
- What happens when 100+ markers would be rendered? At 10km radius this is unlikely, but if it occurs, markers should cluster or the macro-zoom overlay should activate.
- How does the app handle expired page sessions? The app is sessionless in v1 — a page refresh reloads the map from scratch.

## Requirements

### Functional Requirements

- **FR-001**: The web app MUST render a full-screen Leaflet interactive map centered on the driver's location (or Tunis default) constrained to the Tunisia bounding box (30-38°N, 7-12°E)
- **FR-002**: The map MUST fetch nearby stations from `/api/v1/nearby` with viewport coordinate debouncing at 300ms (no API calls during continuous pan/zoom)
- **FR-003**: Station markers MUST use custom pre-loaded vector charging pin icons (stored in `public/markers/`)
- **FR-004**: The app MUST display shimmer loading placeholders during API calls, an error boundary component with retry on failure, and an empty-state message when no stations are found
- **FR-005**: A macro-zoom overlay MUST appear when the user zooms out beyond zoom level 8, blocking marker interaction with a "Zoom in to see charging stations" message
- **FR-006**: Clicking a station marker MUST open a premium sliding drawer (`StationDrawer.tsx`) showing station name, distance, partner name, and address
- **FR-007**: The app MUST be structured under `source/apps/web-driver/` with the specified directory layout (`components/`, `hooks/`, etc.)
- **FR-008**: The app MUST use Tailwind CSS with shared design tokens extending the project's shared primitive system
- **FR-009**: The app MUST include an `ErrorBoundary.tsx` component wrapping the entire map area to catch and display unhandled rendering errors gracefully
- **FR-010**: The `useDebounce.ts` hook MUST debounce viewport changes with a 300ms delay before triggering navigation logic
- **FR-011**: In-flight API requests MUST be cancelled (via `AbortController`) when a new viewport debounce fires before the previous request completes
- **FR-012**: The station drawer MUST be dismissable by clicking outside its area or pressing a close button, with smooth slide animation

### Key Entities

- **MapViewport**: Current map center (lat/lng) and zoom level, debounced at 300ms, constrained to Tunisia bounding box
- **StationMarker**: Leaflet marker with custom charging pin icon at station coordinates, clickable to open StationDrawer
- **StationDrawer**: Sliding panel component displaying selected station details (name, distance, partner, address), with dismiss and animation behaviors
- **ApiFetchState**: Discriminated union of four states — loading (shimmer), success (markers), empty (message), error (boundary + retry)
- **MacroZoomOverlay**: Full-viewport overlay rendered at zoom < 8, blocking map interaction with instructional message
- **ErrorBoundary**: React error boundary class component wrapping the map to catch unhandled rendering exceptions

## Success Criteria

### Measurable Outcomes

- **SC-001**: The map renders and displays station markers within 3 seconds of page load on a standard broadband connection
- **SC-002**: Viewport debouncing at 300ms reduces API calls during continuous panning by at least 90% compared to no debouncing
- **SC-003**: Shimmer placeholders appear within 200ms of initiating an API call and remain visible until data arrives
- **SC-004**: Error boundary catches 100% of unhandled rendering errors and displays a user-friendly fallback UI
- **SC-005**: Macro-zoom overlay activates within 100ms of crossing zoom level 8 threshold and deactivates instantly when zooming back in
- **SC-006**: Station drawer slides open in under 300ms from marker click and slides closed in under 200ms from dismiss action
- **SC-007**: The app maintains 60fps during pan/zoom on modern browsers (Chrome, Firefox, Safari) with up to 50 visible markers

## Assumptions

- The existing `/api/v1/nearby` endpoint (deployed via Traefik) remains stable and accessible at the same host origin
- OpenStreetMap tile server (tile.openstreetmap.org) is used as the tile provider
- Shared design tokens (colors, typography, spacing) are defined in `packages/shared-ui` and consumed by the web driver app
- Tailwind CSS is configured via `tailwind.config.js` extending the shared token primitives
- No authentication is required during the validation phase — the API is publicly accessible (as per Sprint 1.2 CORS configuration)
- Leaflet v1.9+ with `react-leaflet` v5 is used as the mapping library
- Custom charging pin SVGs are pre-loaded in `public/markers/` and do not require an external API
- The app is a single-page application (SPA) served via a simple static file server during validation (Traefik can route to it)
- No state management library beyond React's built-in hooks is needed for v1
