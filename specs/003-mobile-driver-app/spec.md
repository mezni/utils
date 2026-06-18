# Feature Specification: Web Driver Client

**Feature Branch**: `004-web-driver-client`

**Created**: 2026-06-18

**Status**: Draft

**Input**: Sprint 1.4 — Web Driver Client (React + Leaflet) with interactive map, station markers, and Tunisia bounds | **Spec**: [`spec.md`](./spec.md)

## Clarifications

### Session 2026-06-18

- Q: How does the mobile app reach the API from a physical device? → A: Configurable `API_BASE_URL` via Expo app config (`app.json` extras), default documented in quickstart. Developer sets their LAN IP during validation.
- Q: Should location coordinates stored in AsyncStorage be protected? → A: Round viewport center coordinates to 2 decimal places before caching. Station data cached as-is (no personal data).
- Q: Should crash/error reporting be integrated (Sentry etc.)? → A: No crash reporting in v1. Rely on Metro logs and device console during validation testing.
- Q: Authentication & Security → A: Defer authentication to MVP-7. JWT and Keycloak foundation documented in assumptions, but validation uses public API endpoints as currently specified.
- Q: Observability & Debugging → A: Document minimum observable signals needed for validation: API response timing, error categorization (network vs data), and AsyncStorage cache hit/miss rates. No structured logging framework required for v1.
- Q: Localization Support → A: Arabic RTL support marked as "will be implemented" enhancement. Default to English/French for v1 validation to unblock testing.
- Q: API Versioning & Protocol → A: Use HTTP/1.1 with permissive CORS (`*`) during validation to support Expo Go without native build step; specify HTTP/2 and strict CORS in future sprint.
- Q: Accessibility Considerations → A: Add minimal accessibility requirements for v1: ARIA labels for markers/buttons, WCAG AA color contrast (4.5:1), minimum 44px touch targets. Defer advanced features (screen readers, gestures, voice control) to future sprints.

## User Scenarios & Testing

### User Story 1 - Interactive Map with Station Markers (Priority: P1)

A driver opens the web app and sees a full-screen Leaflet map centered on their GPS location (or defaulting to Tunis). Charging stations within 10km are fetched from `/api/v1/nearby` and displayed as custom charging pin markers. The map is constrained to the Tunisia bounding box.

**Why this priority**: The interactive map is the primary interface. Without it, the web app delivers no value. All downstream features depend on this foundation.

**Independent Test**: A driver opens the app in a browser, the map renders at their GPS location (or Tunis default), and station markers appear as charging pin icons. Panning outside Tunisia is blocked.

**Acceptance Scenarios**:

1. **Given** the app is launched, **When** the driver grants location permission, **Then** the map centers on their GPS location at zoom level 14 and fetches nearby stations within 10km
2. **Given** the app is launched without location permission, **When** the driver denies location access, **Then** the map defaults to Tunis (36.8, 10.18) at zoom level 12
3. **Given** station data is returned from the API, **When** markers are rendered, **Then** custom charging pin icons appear at each station's coordinates
4. **Given** the driver pans the map, **When** the viewport center moves, **Then** Tunisia boundary constraints prevent panning outside approx 30-38°N, 7-12°E
5. **Given** station markers are visible, **When** the driver clicks a marker, **Then** an info popup shows the station name and distance

---

### User Story 2 - Viewport Debouncing and Station Refresh (Priority: P2)

Viewport changes (pan/zoom) are debounced by 300ms before triggering a new `/api/v1/nearby` query. This prevents excessive API calls during rapid map interaction.

**Why this priority**: Without debouncing, every pan gesture would fire API calls, overwhelming the network and degrading performance on mobile connections.

**Independent Test**: A driver pans the map rapidly. No API calls are made during the gesture. After panning stops for 300ms, a single API call is made.

**Acceptance Scenarios**:

1. **Given** the driver is panning the map, **When** viewport coordinates change continuously, **Then** no API calls are made until the viewport remains stable for 300ms
2. **Given** the viewport has been stable for 300ms, **When** the debounce timer fires, **Then** a single `/api/v1/nearby` call is made with the new center coordinates
3. **Given** a debounced API call is in flight, **When** the user starts panning again, **Then** the in-flight request is cancelled (no stale data applied)

---

### User Story 3 - Loading, Error, and Empty States with Shimmer (Priority: P2)

During all API interactions, the driver sees polished mobile UX states: shimmer loading placeholders, a styled error boundary with retry button, and an empty-state message when no stations are nearby.

**Why this priority**: The constitution mandates loading/error/empty states for every UI component. On mobile, robust error handling is especially critical given unreliable cellular networks in Tunisia.

**Independent Test**: Each state can be observed by simulation: slow network (shimmer), airplane mode (error with retry), and remote location (empty state).

**Acceptance Scenarios**:

1. **Given** the map is loading nearby stations, **When** the API call is in progress, **Then** shimmer skeleton placeholders are displayed over the map area
2. **Given** the API call fails (10s timeout, any non-2xx, max 3 retries), **When** the error is detected, **Then** a styled error boundary is shown with a user-friendly message and "Retry Connection" button
3. **Given** the API returns zero stations, **When** the empty response is received, **Then** a message is displayed guiding the user to pan towards a major city (Tunis, Sousse, Sfax)
4. **Given** the error boundary is displayed, **When** the driver taps "Retry Connection", **Then** the API call is re-attempted

---

### User Story 4 - AsyncStorage Offline Cache (Priority: P2)

On every successful nearby query, the app writes the station data and viewport coordinates to AsyncStorage. When the device loses connectivity, the app reads the last cached viewport and renders those markers in offline mode with a banner indicating cached data.

**Why this priority**: Mobile networks in Tunisia can be unreliable. Offline cache ensures the app remains usable during brief connectivity gaps, which is critical for driver trust and adoption.

**Independent Test**: A driver successfully loads nearby stations, then switches to airplane mode. The map continues to display the last known stations and a banner appears reading "Viewing cached data."

**Acceptance Scenarios**:

1. **Given** a successful nearby API response is received, **When** the response is processed, **Then** the station data and current viewport center are written to AsyncStorage immediately
2. **Given** the device has no network connectivity, **When** the app attempts to fetch nearby stations and fails, **Then** the app reads the last cached viewport from AsyncStorage and renders those markers
3. **Given** cached data is being displayed, **When** the map renders in offline mode, **Then** a top-bar banner appears: "Viewing cached data. Connect to the internet for real-time status updates."
4. **Given** the device regains connectivity, **When** the driver performs a manual refresh, **Then** fresh data is fetched from the API and the cache is updated

---

### User Story 5 - Macro-Zoom Overlay (Priority: P3)

When the user zooms out past zoom level 8 where individual markers would overlap and degrade performance, an overlay appears across the map with the message "Zoom in closer to view available charging stations." Markers are hidden beneath the overlay.

**Why this priority**: On mobile devices with smaller screens, marker overlap at low zoom levels creates a poor UX and can cause performance issues. This overlay keeps the experience clean.

**Independent Test**: The user pinches to zoom out past the threshold. An overlay covers the map with instructional text. Zooming back in removes the overlay and restores markers.

**Acceptance Scenarios**:

1. **Given** the map is zoomed out beyond zoom level 8, **When** the threshold is crossed, **Then** a full-viewport overlay appears with the message "Zoom in closer to view available charging stations."
2. **Given** the macro-zoom overlay is displayed, **When** the user zooms back in to zoom level 8 or above, **Then** the overlay is removed and station markers are shown
3. **Given** the macro-zoom overlay is active, **When** the user attempts to tap markers beneath it, **Then** interactions are blocked

---

### Edge Cases

- What happens when the app is backgrounded and then returns to foreground? The map should refresh station data (respecting 60-second stale cache threshold).
- What happens on very small screens (iPhone SE, small Android)? The map should fill available space with appropriate safe-area padding.
- What happens during rapid pinch-zoom while a station fetch is in flight? The in-flight request is cancelled (AbortController), debounce resets.
- How does the app handle expired location permissions? The app should gracefully degrade to the Tunis default viewport rather than crashing or showing an error.
- What happens when AsyncStorage is full or corrupted? The app should catch storage errors silently and fall back to online-only mode.
- How does the map perform on low-end Android devices? At zoom levels where markers are visible (≥8), marker counts are expected to stay under 50 within a 10km urban radius.

## Requirements

### Functional Requirements

- **FR-001**: The app MUST render a full-screen `react-native-maps` map constrained to the Tunisia bounding box (30-38°N, 7-12°E), centering on the driver's GPS location or defaulting to Tunis (36.8, 10.18)
- **FR-002**: The app MUST fetch nearby stations from `/api/v1/nearby` using the device's GPS coordinates and a fixed 10km radius, with 300ms viewport debouncing; coordinates MUST pass through shared validation (`isWithinTunisia`) before being included in API query strings
- **FR-003**: Station markers MUST use custom charging pin icons (pre-loaded vector assets in the app bundle)
- **FR-004**: The app MUST display shimmer skeleton placeholders during API calls (replaced using LayoutAnimation on transition), a styled error boundary with "Retry Connection" button on failure (max 3 retries per manual attempt), and an empty-state message guiding users to major cities
- **FR-005**: On every successful API response, the app MUST persist station data and the viewport center (rounded to 2 decimal places for privacy) to AsyncStorage
- **FR-006**: When the device is offline and the API is unreachable, the app MUST fall back to AsyncStorage cached data and display a banner: "Viewing cached data. Connect to the internet for real-time status updates."
- **FR-007**: A macro-zoom overlay MUST appear when zoom level drops below 8, hiding markers and displaying "Zoom in closer to view available charging stations."
- **FR-008**: The app MUST request location permissions via Expo Location API on first launch, degrading gracefully to the Tunis default if denied
- **FR-009**: Tapping a station marker MUST display an info window or callout showing station name, distance, and partner name
- **FR-010**: The app MUST provide pull-to-refresh to manually re-fetch nearby stations (no auto-refresh on pan/zoom)
- **FR-011**: In-flight API requests MUST be cancelled when a new viewport debounce fires before the previous request completes
- **FR-013**: The app MUST accept a configurable `API_BASE_URL` via Expo app config extras (settable in `app.json` or `.env`), default documented in project quickstart, to allow the mobile app to reach the backend from physical devices during validation

### Key Entities

- **MapViewport**: Current map center (lat/lng) and zoom level, debounced at 300ms, constrained to Tunisia bounding box
- **StationMarker**: `react-native-maps` Marker with custom charging pin icon at station coordinates, tappable for callout
- **ApiFetchState**: Discriminated union of four states — loading (shimmer), success (markers), empty (guidance message), error (boundary + retry)
- **AsyncCache**: Persisted station data and viewport in AsyncStorage, keyed by viewport center, with write-on-read pattern
- **OfflineBanner**: Top-bar banner component rendered when displaying cached data in offline mode
- **MacroZoomOverlay**: Full-viewport overlay rendered at zoom < 8, blocking marker interaction with instructional text

## Success Criteria

### Measurable Outcomes

- **SC-001**: The map renders and displays station markers within 4 seconds of launch on a standard 4G connection
- **SC-002**: Viewport debouncing at 300ms reduces API calls during continuous panning by at least 90%
- **SC-003**: Shimmer placeholders appear within 200ms of initiating an API call and are replaced by content or error within 10s
- **SC-004**: AsyncStorage cache write completes in under 100ms after receiving a successful API response
- **SC-005**: Offline fallback reads and renders cached markers in under 500ms when API is unreachable
- **SC-006**: Macro-zoom overlay activates within 200ms of crossing zoom level 8 threshold
- **SC-007**: The app does not crash when location permission is denied, backgrounded and refocused, or AsyncStorage is corrupted
- **SC-008**: All features run successfully inside Expo Go without any custom native build step

## Assumptions

- The existing `/api/v1/nearby` endpoint is accessible via a configurable `API_BASE_URL` (set in Expo app config extras) pointing to the machine running Traefik on the LAN
- OpenStreetMap tiles are rendered via `react-native-maps` default provider (Google Maps on iOS, Google Maps or OSM on Android depending on API key — during validation, default Apple/Google tiles are acceptable)
- No authentication is required during the validation phase (API is publicly accessible)
- Custom charging pin SVGs/PNGs are bundled as static assets in the app package
- Pull-to-refresh uses standard React Native `RefreshControl` pattern
- The app targets both iOS and Android during validation, tested via Expo Go
- Expo SDK 54 is the locked version as specified in the constitution tech stack
- No state management library beyond React built-in hooks is needed for v1
- The app is sessionless in v1 — closing and reopening the app reloads from scratch
- No crash reporting or error tracking service is integrated in v1; debugging is done via Metro logs and device console during validation
- 10km radius is sufficient for urban area coverage; rural areas will see empty states
- Observability for validation: capture API response timing, categorize errors (network vs data), and track AsyncStorage cache hit/miss rates. No structured logging framework required; rely on console debugging during validation phase.
- Localization: Arabic RTL support documented as "will be implemented" enhancement. Default to English/French for v1 validation to unblock testing. Localized strings can be added in future sprint without breaking current implementation.
- API Protocol: Use HTTP/1.1 with permissive CORS (`*`) for validation to ensure Expo Go works without native build step. Upgrade to HTTP/2 and strict CORS in future sprint.
- Accessibility: Minimum requirements for v1 include ARIA labels for interactive elements, WCAG AA color contrast (4.5:1), and 44px minimum touch targets. Advanced accessibility features (screen readers, gestures, voice control) deferred to future sprints.
- Authentication and authorization (Keycloak JWT, driver role) are deferred to MVP-7. Current v1 uses public API endpoints without authentication, per constitution security foundation but validation-phase simplified access
