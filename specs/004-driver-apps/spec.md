# Feature Specification: Mobile & Web Driver Apps

**Feature Branch**: `004-driver-apps`

**Created**: 2026-06-12

**Status**: Draft

**Input**: User description: "read from mvp 1 phase 4"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Discover Stations via Map (Priority: P1)

As a driver, I want to see all charging stations on an interactive map so I can quickly find available charging locations near me.

**Why this priority**: This is the core value proposition - allowing users to visually discover stations is the primary use case that makes the service useful.

**Independent Test**: Can be fully tested by opening the map view, verifying markers render correctly, and confirming a user can tap a marker to see station details. Delivers immediate value of station location discovery.

**Acceptance Scenarios**:

1. **Given** user launches the app, **When** map screen loads with geolocation permission granted, **Then** user sees nearby charging stations as interactive markers on the map
2. **Given** user is at a location, **When** app renders the map, **Then** markers appear within the configured search radius (10km default)
3. **Given** user taps a station marker, **When** interaction completes, **Then** app shows station detail screen with charger information
4. **Given** user is on the map, **When** user pans or zooms, **Then** app updates markers to reflect visible area without re-rendering entire map
5. **Given** map has >1000 stations, **When** user interacts with map, **Then** app maintains smooth performance with no jitter or marker flickering

---

### User Story 2 - Search Stations by Location or Name (Priority: P1)

As a driver, I want to search for stations by name or find ones nearby using my current location so I can quickly locate charging options.

**Why this priority**: Search is essential for users who know what they're looking for (by name) or want to discover options near their current position. This complements the map view and increases discovery utility.

**Independent Test**: Can be fully tested by entering a station name or location, confirming the app queries the backend, and verifying results appear with correct distance/location data. Delivers value of targeted station discovery.

**Acceptance Scenarios**:

1. **Given** user is on search screen, **When** user enters a station name (e.g., "Tunis Central"), **Then** app shows matching stations with distance information
2. **Given** user enables geolocation and searches, **When** app queries for nearby stations, **Then** results show only stations within search radius
3. **Given** user searches with no results, **When** query completes, **Then** app displays empty state with helpful message and option to expand search radius
4. **Given** user searches for invalid input, **When** query executes, **Then** app shows validation error message
5. **Given** user searches while offline, **When** network request fails, **Then** app displays cached results or error state with retry option

---

### User Story 3 - View Station Details & Chargers (Priority: P1)

As a driver, I want to see detailed information about each station including charger types, availability status, and pricing so I can make informed decisions.

**Why this priority**: Users need detailed station information before committing to charge - this is the decision point where they verify compatibility and pricing.

**Independent Test**: Can be fully tested by tapping a station from list or map, verifying all details load, and confirming charger information displays correctly. Delivers value of informed charging decisions.

**Acceptance Scenarios**:

1. **Given** user is on station detail screen, **When** screen renders, **Then** app shows station name, address, opening hours, and amenities
2. **Given** user views station details, **When** app displays charger information, **Then** each charger shows type (CCS, CHAdeMO, AC), connector count, and real-time availability status
3. **Given** user views station details, **When** pricing information is available, **Then** app displays charging rates per kWh or per session
4. **Given** user is on station detail screen, **When** app loads station location on map, **Then** map shows marker at exact station coordinates
5. **Given** user taps a charger type, **When** interaction completes, **Then** app shows estimated charging time based on typical power levels

---

### User Story 4 - Navigate to Station (Priority: P2)

As a driver, I want to get turn-by-turn navigation directions to selected stations so I can easily travel there.

**Why this priority**: Navigation reduces friction in the charging journey and is highly valuable but can be considered a second-order feature after core discovery and details work.

**Independent Test**: Can be fully tested by selecting a station and verifying navigation opens with correct destination. Delivers value of travel planning convenience.

**Acceptance Scenarios**:

1. **Given** user views station details, **When** user taps navigation button, **Then** app opens external navigation app with station address as destination
2. **Given** user is on map view, **When** user taps a station marker, **Then** app shows navigation option if station has address information
3. **Given** user is offline, **When** navigation button is tapped, **Then** app displays error message explaining offline limitation

---

### User Story 5 - Switch Between Light and Dark Mode (Priority: P2)

As a user, I want to switch between light and dark themes so the app remains comfortable to use in any lighting condition.

**Why this priority**: Dark mode is important for user experience but users can adapt to light mode - this is a quality-of-life feature rather than a core functionality requirement.

**Independent Test**: Can be fully tested by toggling dark mode and verifying all screens render correctly in both themes. Delivers value of personalized user experience.

**Acceptance Scenarios**:

1. **Given** user is in light mode, **When** user toggles to dark mode, **Then** all screen backgrounds, text, and UI elements switch to dark theme colors
2. **Given** user is in dark mode, **When** user toggles to light mode, **Then** all elements switch to light theme with proper contrast ratios
3. **Given** app loads, **When** theme preference is set in system, **Then** app respects system default or user preference
4. **Given** user changes theme, **When** user navigates between screens, **Then** theme persists across navigation

---

### User Story 6 - Refresh Data and Load More (Priority: P2)

As a driver, I want to manually refresh the station list and load additional stations via pagination so I can always see the most current information.

**Why this priority**: Manual refresh provides users control over data freshness, and pagination is essential for managing large station lists without performance issues.

**Independent Test**: Can be fully tested by pulling to refresh, verifying data updates, and confirming pagination loads additional items. Delivers value of current data and better long-list handling.

**Acceptance Scenarios**:

1. **Given** user is on station list, **When** user performs pull-to-refresh gesture, **Then** app shows skeleton loading state and updates list with latest data
2. **Given** station list has additional items, **When** user scrolls to bottom, **Then** app loads next page of stations without losing current view
3. **Given** user is offline, **When** pull-to-refresh is triggered, **Then** app shows offline indicator and does not refresh
4. **Given** user refreshes successfully, **When** request completes, **Then** app displays a brief success toast notification

---

### User Story 7 - Responsive Web Version (Priority: P3)

As a user, I want to access the same station discovery functionality via a responsive web browser so I can use the service on desktop devices without a mobile app.

**Why this priority**: Desktop support is valuable for users who primarily access the web, but the primary target audience is mobile users. This can be implemented after mobile apps are validated.

**Independent Test**: Can be fully tested by accessing the web app on different screen sizes and verifying all features work responsively. Delivers value of multi-platform accessibility.

**Acceptance Scenarios**:

1. **Given** user opens web app on desktop, **When** app loads, **Then** view adapts to full desktop width with optimized layout
2. **Given** user opens web app on tablet, **When** view renders, **Then** layout adjusts to tablet-specific dimensions
3. **Given** user opens web app on mobile device, **When** view loads, **Then** app behaves identically to native mobile experience
4. **Given** user resizes browser window, **When** window changes size, **Then** app layout adjusts smoothly without breaking

---

### Edge Cases

- What happens when geolocation permission is denied? App falls back to center map on default location (Tunis coordinates) and shows informative error message
- How does system handle network timeout or connection failure? App displays cached data if available, shows error screen with retry button, and provides feedback about network issues
- What happens if user searches for a location with no charging stations? App displays empty state with suggestion to expand search radius or try different location
- How does app handle very large station lists (1000+ stations)? App uses virtualization or pagination to prevent UI lag, maintains performance with skeleton loading during fetch
- What happens if user's device is low on storage or memory? App shows loading states during transitions, uses memory-efficient rendering, and prompts user to close background apps if performance issues occur
- How does app handle timezone changes while viewing station details? App displays opening hours based on user's current timezone, not server timezone
- What happens when OSM Nominatim API rate limits are exceeded? App displays user-friendly error message, implements exponential backoff retry with increasing delays, and informs user of rate limit policy
- How does app handle concurrent offline cache updates when network returns? App marks cache as stale and queues updates, applying changes in order to maintain data consistency without overwriting newer server data
- What happens when geocoding query matches multiple locations? App shows list of candidate locations with distance/score, allows user to select correct location, falls back to first result with warning if none selected

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Mobile app MUST be built using Expo SDK 54
- **FR-002**: Mobile app MUST use file-based routing with expo-router v3
- **FR-003**: Mobile app MUST use Zustand for UI state management (dark mode, filters, map state)
- **FR-004**: Mobile app MUST use React Query for API data fetching and caching
- **FR-005**: Mobile app MUST consume @bornemap/tokens and @bornemap/ui packages from workspace
- **FR-006**: Mobile app MUST implement skeleton screens for all data-loading states
- **FR-007**: Mobile app MUST provide haptic feedback on all primary actions (buttons, markers)
- **FR-008**: Mobile app MUST use react-native-maps (open-source, no API key needed) for map rendering
- **FR-009**: Mobile app MUST use Leaflet via react-native-webview for web app map
- **FR-010**: Mobile app MUST support dark mode via ThemeProvider component
- **FR-011**: Mobile app MUST implement pull-to-refresh on station list and map views
- **FR-012**: Mobile app MUST display station list with pagination (page, per_page parameters)
- **FR-013**: Mobile app MUST support geocoding via OSM Nominatim API with 10s timeout, 2 retries, linear backoff
- **FR-014**: Mobile app MUST perform radius-based search with default 10km
- **FR-015**: Mobile app MUST dynamically expand search radius to 25km if fewer than 5 stations are found
- **FR-016**: Mobile app MUST show station detail screen with charger information, pricing, and hours
- **FR-017**: Mobile app MUST provide navigation option to station address with error message and copy-to-clipboard button
- **FR-018**: Mobile app MUST use bottom sheet for station preview on map view with swipe gestures for dismissal
- **FR-019**: Mobile app MUST cache last 50 stations + station details for offline access
- **FR-020**: Mobile app MUST use reanimated v3 for smooth UI transitions and animations
- **FR-021**: Mobile app MUST implement optimistic UI updates for user interactions (favorites, selections)
- **FR-022**: Mobile app MUST provide contextual error messages with recovery actions
- **FR-023**: Mobile app MUST log API requests and responses with structured JSON format for observability
- **FR-024**: Mobile app MUST track fetch times and success rates for each API endpoint in logs
- **FR-025**: Mobile app MUST maintain smooth performance with 1000+ stations on map
- **FR-026**: Mobile app MUST use system-provided map markers for station locations with clustering badges for dense areas
- **FR-027**: Mobile app MUST show station distance in kilometers from user's location
- **FR-028**: Mobile app MUST display charger types (CCS, CHAdeMO, AC) with connector count
- **FR-029**: Mobile app MUST show real-time availability status for each charger
- **FR-030**: Mobile app MUST load station images lazily only when station detail is visible
- **FR-031**: Mobile app MUST persist dark mode preference in AsyncStorage (React Native) + localStorage (Web)
- **FR-032**: Mobile app MUST use only skeleton screens (no global spinner for subsequent navigations)
- **FR-033**: Mobile app MUST use manual refresh only (no automatic refresh for station data)
- **FR-040**: Web app MUST use React 19 with Leaflet for map rendering
- **FR-041**: Web app MUST support responsive design with mobile-first approach
- **FR-042**: Web app MUST consume @bornemap/tokens and @bornemap/ui packages from workspace
- **FR-043**: Web app MUST implement same station discovery flows as mobile app (map, list, search, details)
- **FR-044**: Web app MUST provide navigation buttons to station address via external mapping services
- **FR-045**: Web app MUST handle dark mode via CSS variables from tokens package
- **FR-046**: Web app MUST implement pull-to-refresh pattern for station list
- **FR-047**: Web app MUST show pagination controls for station list

### Key Entities

- **Station**: Physical location where charging occurs. Attributes: id, name, address, geometry (latitude, longitude), amenities list, operating_hours, created_at, updated_at
- **Charger**: Individual charging unit at a station. Attributes: id, station_id, charger_type (CCS/CHAdeMO/AC), connector_count, availability_status (available/in_use/maintenance), power_kw, is_active
- **StationImage**: Visual documentation of station. Attributes: id, station_id, url, caption, is_primary

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Users can discover and view charging stations in under 3 seconds after app launch
- **SC-002**: Map with 1000+ stations renders smoothly with no jitter or marker flickering
- **SC-003**: Station search completes in under 500ms for radius queries
- **SC-004**: Station detail page loads in under 200ms for any station with available data
- **SC-005**: 100% of critical paths use skeleton screens instead of spinners
- **SC-006**: 100% of primary actions provide haptic feedback on mobile
- **SC-007**: Dark mode transitions complete in under 300ms with smooth color changes
- **SC-008**: Pull-to-refresh gesture provides visual feedback and updates data correctly
- **SC-009**: App maintains 60fps performance during all transitions and interactions
- **SC-010**: Both mobile and web apps work on primary mobile devices (iOS 13+, Android 10+, 2GB RAM minimum)

## Assumptions

- Users have stable internet connectivity for most use cases; offline support is secondary (cached last 50 stations)
- Mobile app users will primarily access station details on mobile devices
- Navigation functionality will use external apps (Google Maps, Apple Maps, Waze) rather than building custom routing
- Dark mode is implemented at the design system level (@bornemap/ui) and consumed by both apps
- Theme preference will be persisted in AsyncStorage (React Native) and localStorage (Web) for cross-platform consistency
- All station data is already available via driver-service API endpoints (endpoints validated in Phase 2)
- Geocoding will be handled via public OSM Nominatim API with 10s timeout, 2 retries, linear backoff strategy
- Apps will implement structured JSON logging for API requests, responses, fetch times, and error tracking for debugging and production monitoring
- OSM Nominatim rate limit handling will use exponential backoff retry (10s, 30s, 60s delays) with user-friendly error messages
- Geocoding queries that return multiple locations will display candidate list with distance/score for user selection
- Both mobile and web apps will share most UI components and logic via design system packages
- Station availability updates are handled by backend services; apps display current data without real-time subscriptions
- Pagination will use page-based approach with page/limit parameters, not cursor-based
- Users will have geolocation permission enabled; if denied, app will show informative error and fallback to default location
- Web app will remain public-access (no authentication required) to maximize accessibility
- Manual refresh will be the only data refresh method (no automatic background refresh)
- Map marker clustering will be implemented with 50m radius clustering and badge counts
- Station images will be loaded lazily only when station detail view becomes visible
