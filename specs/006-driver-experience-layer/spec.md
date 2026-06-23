# Feature Specification: Driver Experience Layer (UX + Product Polish)

**Feature Branch**: `006-driver-experience-layer`

**Created**: 2026-06-22

**Status**: Draft

**Input**: Full Sprint 5 — Deliver a high-performance, production-grade driver experience: map usability becomes fast and offline-resilient, personalization is introduced (favorites, preferences), and the frontend becomes polished while remaining strictly data-consumer only.

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Driver Favorites System

**Goal**: Drivers can save, view, and remove favorite charging stations for quick access. Favorites persist across sessions and devices. Favorites are user-generated relational data stored separately from preferences in the existing JSONB column.

**Why this priority**: Favorites are the highest-impact personalization feature — they reduce recurring search time, encourage loyalty, and are a prerequisite for downstream features (smart notifications, usage analytics).

**Independent Test**: Open the app, navigate to a station, tap the favorite heart icon, verify the station appears in a "Favorites" list. Unfavorite it, verify it disappears. Refresh the app, verify favorites persist.

**Acceptance Scenarios**:

1. **Given** a driver is viewing a station detail page, **When** they tap the favorite button, **Then** the station is added to their favorites list and the UI updates optimistically (heart fills within 150ms)
2. **Given** a driver has favorited stations, **When** they navigate to the Favorites section, **Then** all favorited stations display with current availability and distance
3. **Given** a driver wants to remove a favorite, **When** they tap the unfavorite button, **Then** the station is removed from their list and the UI updates optimistically
4. **Given** a driver logs out and logs back in, **When** they access the Favorites section, **Then** all previously saved favorites are restored from the server
5. **Given** a driver without network connectivity, **When** they access their favorites, **Then** the cached favorites list is displayed from local storage
6. **Given** two devices are logged into the same account, **When** a favorite is added or removed on one device while the other is offline, **Then** conflicts are resolved using last-write-wins semantics upon reconnection

### User Story 2 — User Preferences System

**Goal**: Drivers can customize their app experience by setting preferred charger types, map filters, and other personalization options. Preferences are stored in the existing users.preferences JSONB column, in a section separate from favorites.

**Why this priority**: Personalization reduces friction and improves satisfaction by letting drivers tailor the app to their habits without requiring schema expansion.

**Independent Test**: Open Settings, change preferred charger type from "CCS" to "CHAdeMO", verify the map filters to show only CHAdeMO stations. Change preferences, refresh, verify persistence.

**Acceptance Scenarios**:

1. **Given** a driver opens Settings, **When** they select a preferred charger type (CCS/CHAdeMO/Type 2), **Then** the map filters to show only stations matching that connector type
2. **Given** a driver sets map filter preferences (max distance, availability, connector type), **When** they return to the map, **Then** the map automatically applies their saved filters
3. **Given** a driver adjusts their preferred region or default view, **When** they open the app, **Then** the map centers on their saved last region
4. **Given** preferences are saved, **When** the driver checks Settings, **Then** all preferences persist correctly in the preferences section of the users.preferences JSONB column
5. **Given** a driver clears preferences, **When** they reset to defaults, **Then** the app returns to the default map view and filter settings

### User Story 3 — Offline Cache Layer

**Goal**: The app functions gracefully without network connectivity by caching stations, favorites, and preferences locally. Only previously viewed map tiles are available offline. Backend dependencies are never required for offline functionality.

**Why this priority**: EV charging is inherently mobile — drivers frequently lose signal in garages, tunnels, and remote areas. Offline capability is essential for a reliable experience.

**Independent Test**: Load the app with connectivity, favorite some stations. Enable airplane mode. Verify favorites are accessible, the map shows cached data, and the app does not crash or show blank screens.

**Acceptance Scenarios**:

1. **Given** a driver has previously loaded the app, **When** they enter an area with no connectivity, **Then** the app displays cached station data and previously viewed map tiles
2. **Given** a driver has no network, **When** they view their favorites, **Then** favorites are served from local cache and are fully interactive
3. **Given** connectivity is restored, **When** the driver interacts with the app, **Then** the app syncs pending changes using last-write-wins conflict resolution and refreshes stale cache data automatically
4. **Given** the app is in offline mode, **When** the driver searches for stations, **Then** search queries execute against the local cache for previously loaded data
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

**Goal**: Drivers can search for stations by name, address, or partial match. Online search executes via driver-service using Postgres trigram search. Offline search runs against the local cache. Both handle typos and abbreviations.

**Why this priority**: Search is the primary navigation method for finding specific stations. Fuzzy matching ensures drivers find what they need even when they don't know the exact name. Server-side search preserves the consumer-only frontend pattern and handles large datasets efficiently.

**Independent Test**: Search for "fast charg" — verify it returns stations named "Fast Charging Hub". Search for "IKEA" — verify it returns stations near IKEA locations. Misspell a station name — verify relevant results still appear. Go offline, search again — verify cached results are returned.

**Acceptance Scenarios**:

1. **Given** a driver is online and types "fast charg", **When** partial text is entered, **Then** the query is sent to driver-service which executes a Postgres trigram search and returns matching stations
2. **Given** a driver types "Bern" with an extra space, **When** the search is submitted, **Then** fuzzy matching still returns stations in or near Bern
3. **Given** a driver searches by address, **When** they type "Bahnhofstrasse", **Then** results include stations at that street address
4. **Given** a driver types a 3-character query, **When** search is initiated, **Then** results return within 1 second at P95 matching partial station names or addresses
5. **Given** no results match a search query, **When** results are empty, **Then** a friendly message appears suggesting to broaden the search
6. **Given** a driver performs a search, **When** they select a result, **Then** the map centers on that station and shows the preview card
7. **Given** a driver is offline and searches, **When** the query is submitted, **Then** search executes against the local cache for previously downloaded station data

### User Story 6 — Skeleton Loaders & Optimistic UI

**Goal**: Every screen transition feels instant. Content loads with skeleton placeholders within 150ms, and actions (favorite, search) update the UI optimistically within 150ms without waiting for server confirmation.

**Why this priority**: Perceived performance is the strongest driver of user satisfaction. The <150ms skeleton rule ensures the app never feels slow, even on poor connections.

**Independent Test**: Trigger a station list load — verify skeleton cards appear within 150ms and fill with real content as it arrives. Tap favorite — verify the heart fills within 150ms (optimistic UI), even if the server is slow.

**Acceptance Scenarios**:

1. **Given** a driver navigates to any screen that loads data, **When** data is being fetched, **Then** skeleton placeholder elements appear within 150ms matching the layout of the content being loaded
2. **Given** a driver taps the favorite button, **When** the tap is registered, **Then** the UI updates optimistically within 150ms (heart fills immediately) without waiting for the server response
3. **Given** a driver performs a search, **When** results are loading, **Then** skeleton search result cards appear until real results replace them
4. **Given** an optimistic update fails (server error), **When** the error is detected, **Then** the UI reverts to the previous state gracefully
5. **Given** data is cached locally, **When** the cached data exists, **Then** it is displayed immediately while fresh data loads in the background

### User Story 7 — Session Continuity

**Goal**: The app remembers the driver's last session state: map position, applied filters, and last viewed region. This is UI session state only — authentication lifetime remains managed by Keycloak and is not modified by continuity features.

**Why this priority**: Drivers frequently switch between the app and other apps (navigation, calls). Session continuity eliminates friction in returning to the app without impacting the existing authentication model.

**Independent Test**: Set a map filter (e.g., "CCS only"), zoom to a specific region, close the app. Reopen — verify the map restores the same position and filters. Confirm the Keycloak authentication session is unaffected.

**Acceptance Scenarios**:

1. **Given** a driver has set map filters, **When** they close and reopen the app, **Then** the previously applied UI filters are restored automatically
2. **Given** a driver was viewing a specific map region, **When** they return to the app, **Then** the map centers on the previously viewed region at the same zoom level
3. **Given** a driver was browsing the Favorites list, **When** they return, **Then** they are returned to the last viewed section of the app
4. **Given** a driver's UI session state was set, **When** the app is reopened within 30 minutes, **Then** the UI state is restored without re-navigation
5. **Given** a Keycloak authentication token expires, **When** the app detects expiry, **Then** the standard authentication flow is triggered independently of the session continuity feature

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The system must allow drivers to favorite a station via a single tap with optimistic UI update (heart fills within 150ms)
- **FR-002**: The system must persist favorites to the server via driver-service APIs and synchronize across sessions and devices
- **FR-003**: The system must store favorites in a dedicated `favorites` section within the existing users.preferences JSONB column, separate from the `preferences` section — no new tables or schema expansion
- **FR-004**: The system must provide CRUD operations for user preferences (charger type, map filters, default region) stored in the `preferences` section of users.preferences JSONB
- **FR-005**: The system must cache station data, favorites, and preferences locally for offline access
- **FR-006**: The system must display cached data immediately when network is unavailable
- **FR-007**: The system must automatically sync pending changes using last-write-wins conflict resolution when connectivity is restored
- **FR-008**: The map must cluster station markers at zoom-out levels based on geographic proximity
- **FR-009**: Tapping a station marker must display a preview card with name, connector types, availability, and distance
- **FR-010**: The system must support free-text station search that matches by name, address, and partial text
- **FR-011**: The search must use fuzzy matching to handle typos and misspellings — online via driver-service Postgres trigram search, offline against local cache
- **FR-012**: Search results must return within 1 second at P95 for typical queries
- **FR-013**: Every screen loading data must display skeleton placeholders within 150ms
- **FR-014**: Favorites and search actions must update the UI optimistically within 150ms before server confirmation
- **FR-015**: The system must restore the last map position, zoom level, and applied UI filters on app restart
- **FR-016**: The system must restore the last viewed section on app restart
- **FR-017**: UI session state (map position, filters, last section) must be restored for 30 minutes after app close — authentication session lifetime remains managed by Keycloak and is not modified
- **FR-018**: Favorites synchronization conflicts across devices must be resolved using last-write-wins semantics based on server timestamp
- **FR-019**: Online search must execute via driver-service using Postgres trigram search; offline search must execute against the local cache
- **FR-020**: Only previously downloaded map tiles must be available in offline mode — full offline map coverage is not in scope
- **FR-021**: Favorite, search, preference change, and offline-mode events must emit telemetry events into the existing driver-service telemetry pipeline (analytics_db)
- **FR-022**: driver-service owns all favorites APIs (POST/GET/DELETE /api/v1/driver/favorites) — no other service implements favorites endpoints
- **FR-023**: Authentication session lifetime must remain managed by Keycloak and must not be modified by session continuity features

### Key Entities *(include if feature involves data)*

- **UserFavorites**: Collection of station IDs associated with a user, stored in a dedicated `favorites` section within users.preferences JSONB — separate from `preferences` — owned by driver-service
- **UserPreferences**: User-customizable settings (preferred charger type, map filters, default region) stored in the `preferences` section of users.preferences JSONB
- **OfflineCache**: Local persistent storage for station data, favorites, preferences, and previously viewed map tiles — no backend dependency required for access
- **StationSearchResult**: Search response from driver-service containing matching stations with relevance score, distance, and availability
- **SessionState**: Last app UI state including map position, filters, and active section — stored locally, not in authentication session
- **TelemetryEvent**: Events emitted for favorite_added, favorite_removed, search_executed, search_selected, filter_changed, offline_mode_entered — fed into driver-service telemetry pipeline

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Drivers can favorite/unfavorite a station with optimistic UI visible within 150ms (heart fills immediately, server response may follow)
- **SC-002**: Favorites persist across app restarts and device changes
- **SC-003**: Preferences are restored correctly 100% of the time on app launch
- **SC-004**: The app functions fully offline for cached stations and favorites without requiring any backend dependency
- **SC-005**: Map rendering stays under the frame budget with no jank during zoom or pan operations
- **SC-006**: Search queries return relevant results (including fuzzy matches) within 1 second at P95
- **SC-007**: Skeleton placeholders appear within 150ms on all data-loading screens
- **SC-008**: Optimistic UI updates succeed without visible flicker or rollback in 95%+ of actions
- **SC-009**: UI session state (map position, filters, section) is restored correctly on app restart — authentication session is unaffected
- **SC-010**: No new backend tables or schema changes are introduced — all personalization uses the existing users.preferences JSONB column with separate sections for preferences and favorites

## Assumptions

- The app targets both mobile (React Native via Expo) and web platforms, sharing the same backend APIs
- The existing users.preferences JSONB column supports arbitrary nested JSON structures with separate top-level keys for `preferences` and `favorites`
- Offline storage uses platform-native storage (AsyncStorage for mobile, IndexedDB for web)
- Map clustering is handled client-side using available map SDK features — no new rendering engines
- Map UX improvements (clustering, custom markers, preview cards) use the existing map library
- Online search uses driver-service → Postgres trigram (pg_trgm) indexing — no external search service
- Offline search runs locally against cached station data for previously loaded stations only
- Skeleton loaders are implemented as shared components in the ui-kit library
- Session continuity stores UI state locally (last position, filters, section) and restores from local cache on startup — authentication state is managed separately by Keycloak
- driver-service owns favorites APIs (POST/GET/DELETE /api/v1/driver/favorites) — following the existing GIS domain ownership pattern
- Telemetry events follow the existing event schema from Sprint 3 and are ingested into analytics_db via driver-service
- Only previously viewed map tiles are available offline — no full offline map download
- Favorites conflict resolution uses last-write-wins based on server timestamp
- No changes to server topology, database schema, or service boundaries
- All frontend code remains data-consumer-only with no business logic leakage
