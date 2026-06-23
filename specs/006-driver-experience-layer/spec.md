# Feature Specification: Driver Experience Layer (UX + Product Polish)

**Feature Branch**: `006-driver-experience-layer`

**Created**: 2026-06-22

**Status**: Draft

**Input**: Full Sprint 5 — Deliver a high-performance, production-grade driver experience: map usability becomes fast and offline-resilient, personalization is introduced (favorites, preferences), and the frontend becomes polished while remaining strictly data-consumer only.

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Driver Favorites System

**Goal**: Drivers can save, view, and remove favorite charging stations for quick access. Favorites persist across sessions and devices.

**Why this priority**: Favorites are the highest-impact personalization feature — they reduce recurring search time, encourage loyalty, and are a prerequisite for downstream features (smart notifications, usage analytics).

**Independent Test**: Open the app, navigate to a station, tap the favorite heart icon, verify the station appears in a "Favorites" list. Unfavorite it, verify it disappears. Refresh the app, verify favorites persist.

**Acceptance Scenarios**:

1. **Given** a driver is viewing a station detail page, **When** they tap the favorite button, **Then** the station is added to their favorites list and the UI updates optimistically within 150ms
2. **Given** a driver has favorited stations, **When** they navigate to the Favorites section, **Then** all favorited stations display with current availability and distance
3. **Given** a driver wants to remove a favorite, **When** they tap the unfavorite button, **Then** the station is removed from their list and the UI updates optimistically
4. **Given** a driver logs out and logs back in, **When** they access the Favorites section, **Then** all previously saved favorites are restored from the server
5. **Given** a driver without network connectivity, **When** they access their favorites, **Then** the cached favorites list is displayed from local storage

### User Story 2 — User Preferences System

**Goal**: Drivers can customize their app experience by setting preferred charger types, map filters, and other personalization options. Preferences are stored in the existing users.preferences JSONB column.

**Why this priority**: Personalization reduces friction and improves satisfaction by letting drivers tailor the app to their habits without requiring schema expansion.

**Independent Test**: Open Settings, change preferred charger type from "CCS" to "CHAdeMO", verify the map filters to show only CHAdeMO stations. Change preferences, refresh, verify persistence.

**Acceptance Scenarios**:

1. **Given** a driver opens Settings, **When** they select a preferred charger type (CCS/CHAdeMO/Type 2), **Then** the map filters to show only stations matching that connector type
2. **Given** a driver sets map filter preferences (max distance, availability, connector type), **When** they return to the map, **Then** the map automatically applies their saved filters
3. **Given** a driver adjusts their preferred region or default view, **When** they open the app, **Then** the map centers on their saved last region
4. **Given** preferences are saved, **When** the driver checks Settings, **Then** all preferences persist correctly in the existing users.preferences JSONB schema
5. **Given** a driver clears preferences, **When** they reset to defaults, **Then** the app returns to the default map view and filter settings

### User Story 3 — Offline Cache Layer

**Goal**: The app functions gracefully without network connectivity by caching stations, favorites, and preferences locally. Backend dependencies are never required for offline functionality.

**Why this priority**: EV charging is inherently mobile — drivers frequently lose signal in garages, tunnels, and remote areas. Offline capability is essential for a reliable experience.

**Independent Test**: Load the app with connectivity, favorite some stations. Enable airplane mode. Verify favorites are accessible, the map shows cached data, and the app does not crash or show blank screens.

**Acceptance Scenarios**:

1. **Given** a driver has previously loaded the app, **When** they enter an area with no connectivity, **Then** the app displays cached station data and previously loaded map tiles
2. **Given** a driver has no network, **When** they view their favorites, **Then** favorites are served from local cache and are fully interactive
3. **Given** connectivity is restored, **When** the driver interacts with the app, **Then** the app syncs pending changes and refreshes stale cache data automatically
4. **Given** the app is in offline mode, **When** the driver searches for stations, **Then** search queries against the local cache return results for previously loaded data
5. **Given** the app launches for the first time, **When** there is no network, **Then** the app shows a friendly offline message with retry capability
6. **Given** cached data exists, **When** the app is force-closed and reopened, **Then** cached data is restored from persistent local storage

### User Story 4 — Map UX Upgrade

**Goal**: The map is fast, smooth, and informative. Stations display with clustering at zoom-out levels, smooth transitions between zoom levels, and preview cards on tap.

**Why this priority**: The map is the primary interface for the driver experience. A sluggish or cluttered map is the top reason users abandon the app.

**Independent Test**: Open the map at country zoom level — verify stations cluster. Zoom in — verify clusters break apart smoothly. Tap a cluster — verify it zooms to show individual stations. Tap a station — verify a preview card appears.

**Acceptance Scenarios**:

1. **Given** the map is at a zoom level showing many stations, **When** stations overlap, **Then** they are grouped into clusters with a count badge
2. **Given** a driver taps a station marker, **When** the tap is registered, **Then** a preview card slides up showing station name, connector types, availability, and distance
3. **Given** a driver zooms in or out, **When** the animation completes, **Then** the transition is smooth without jank or frame drops
4. **Given** the map is loaded, **When** station markers are rendered, **Then** markers are customized by connector type (color-coded) and availability (green/orange/red)
5. **Given** a driver pans the map, **When** new stations enter the viewport, **Then** station markers load progressively without blocking map interaction

### User Story 5 — Station Search with Fuzzy Matching

**Goal**: Drivers can search for stations by name, address, or partial match. The search handles typos, abbreviations, and returns relevant results quickly.

**Why this priority**: Search is the primary navigation method for finding specific stations. Fuzzy matching ensures drivers find what they need even when they don't know the exact name.

**Independent Test**: Search for "fast charg" — verify it returns stations named "Fast Charging Hub". Search for "IKEA" — verify it returns stations near IKEA locations. Misspell a station name — verify relevant results still appear.

**Acceptance Scenarios**:

1. **Given** a driver types "fast charg", **When** partial text is entered, **Then** the system returns stations matching "Fast Charging Hub" and similar names
2. **Given** a driver types "Bern" with an extra space, **When** the search is submitted, **Then** fuzzy matching still returns stations in or near Bern
3. **Given** a driver searches by address, **When** they type "Bahnhofstrasse", **Then** results include stations at that street address
4. **Given** a driver types a 3-character query, **When** search is initiated, **Then** results return within 1 second matching partial station names or addresses
5. **Given** no results match a search query, **When** results are empty, **Then** a friendly message appears suggesting to broaden the search
6. **Given** a driver performs a search, **When** they select a result, **Then** the map centers on that station and shows the preview card

### User Story 6 — Skeleton Loaders & Optimistic UI

**Goal**: Every screen transition feels instant. Content loads with skeleton placeholders within 150ms, and actions (favorite, search) update the UI immediately without waiting for server confirmation.

**Why this priority**: Perceived performance is the strongest driver of user satisfaction. The <150ms skeleton rule ensures the app never feels slow, even on poor connections.

**Independent Test**: Trigger a station list load — verify skeleton cards appear within 150ms and fill with real content as it arrives. Tap favorite — verify the heart fills immediately, even if the server is slow.

**Acceptance Scenarios**:

1. **Given** a driver navigates to any screen that loads data, **When** data is being fetched, **Then** skeleton placeholder elements appear within 150ms matching the layout of the content being loaded
2. **Given** a driver taps the favorite button, **When** the request is sent to the server, **Then** the UI updates immediately (heart fills) without waiting for the server response
3. **Given** a driver performs a search, **When** results are loading, **Then** skeleton search result cards appear until real results replace them
4. **Given** an optimistic update fails (server error), **When** the error is detected, **Then** the UI reverts to the previous state gracefully
5. **Given** data is cached locally, **When** the cached data exists, **Then** it is displayed immediately while fresh data loads in the background

### User Story 7 — Session Continuity

**Goal**: The app remembers the driver's last session state: map position, applied filters, and last viewed region. Returning to the app feels seamless — no need to re-navigate or re-configure.

**Why this priority**: Drivers frequently switch between the app and other apps (navigation, calls). Session continuity eliminates friction in returning to the app.

**Independent Test**: Set a map filter (e.g., "CCS only"), zoom to a specific region, close the app. Reopen — verify the map restores the same position and filters.

**Acceptance Scenarios**:

1. **Given** a driver has set map filters, **When** they close and reopen the app, **Then** the previously applied filters are restored automatically
2. **Given** a driver was viewing a specific map region, **When** they return to the app, **Then** the map centers on the previously viewed region at the same zoom level
3. **Given** a driver was browsing the Favorites list, **When** they return, **Then** they are returned to the last viewed section of the app
4. **Given** a driver's session was active, **When** the app is reopened within 30 minutes, **Then** the session is restored without requiring re-authentication

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The system must allow drivers to favorite a station via a single tap with optimistic UI update
- **FR-002**: The system must persist favorites to the server and synchronize across sessions
- **FR-003**: The system must store favorites in the existing users.preferences JSONB column (no new tables or schema expansion)
- **FR-004**: The system must provide CRUD operations for user preferences (charger type, map filters, default region)
- **FR-005**: The system must cache station data, favorites, and preferences locally for offline access
- **FR-006**: The system must display cached data immediately when network is unavailable
- **FR-007**: The system must automatically sync pending changes when connectivity is restored
- **FR-008**: The map must cluster station markers at zoom-out levels based on geographic proximity
- **FR-009**: Tapping a station marker must display a preview card with name, connector types, availability, and distance
- **FR-010**: The system must support free-text station search that matches by name, address, and partial text
- **FR-011**: The search must use fuzzy matching to handle typos and misspellings
- **FR-012**: Search results must return within 1 second for typical queries
- **FR-013**: Every screen loading data must display skeleton placeholders within 150ms
- **FR-014**: Favorites and search actions must update the UI optimistically before server confirmation
- **FR-015**: The system must restore the last map position, zoom level, and applied filters on app restart
- **FR-016**: The system must restore the last viewed section on app restart
- **FR-017**: Active sessions must persist for at least 30 minutes without requiring re-authentication

### Key Entities *(include if feature involves data)*

- **UserFavorites**: Collection of station IDs associated with a user, stored in users.preferences JSONB
- **UserPreferences**: User-customizable settings (preferred charger type, map filters, default region) stored in users.preferences JSONB
- **OfflineCache**: Local persistent storage for station data, favorites, preferences, and map tiles
- **StationSearchResult**: Search response containing matching stations with relevance score, distance, and availability
- **SessionState**: Last app state including map position, filters, and active section

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Drivers can favorite/unfavorite a station with <150ms UI response time
- **SC-002**: Favorites persist across app restarts and device changes
- **SC-003**: Preferences are restored correctly 100% of the time on app launch
- **SC-004**: The app functions fully offline for cached stations and favorites without requiring any backend dependency
- **SC-005**: Map rendering stays under the frame budget with no jank during zoom or pan operations
- **SC-006**: Search queries return relevant results (including fuzzy matches) within 1 second
- **SC-007**: Skeleton placeholders appear within 150ms on all data-loading screens
- **SC-008**: Optimistic UI updates succeed without visible flicker or rollback in 95%+ of actions
- **SC-009**: Session state (map position, filters, section) is restored correctly on app restart
- **SC-010**: No new backend tables or schema changes are introduced — all personalization uses existing users.preferences JSONB

## Assumptions

- The app targets both mobile (React Native via Expo) and web platforms, sharing the same backend APIs
- The existing users.preferences JSONB column supports arbitrary nested JSON structures
- Offline storage uses platform-native storage (AsyncStorage for mobile, IndexedDB for web)
- Map clustering is handled client-side using available map SDK features
- Search is implemented client-side with a backend API endpoint for station data — full-text search is handled via SQLx queries, not an external search service
- Map UX improvements use the existing map library features (clustering, custom markers) without introducing new rendering engines
- Skeleton loaders are implemented as shared components in the ui-kit library
- Session continuity stores state locally (last position, filters) and restores from local cache on startup
- Favorites API follows the existing RESTful pattern (POST/GET/DELETE /api/v1/driver/favorites)
- No changes to server topology, database schema, or service boundaries
- All frontend code remains data-consumer-only with no business logic leakage
