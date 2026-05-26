# Feature Specification: Mobile Driver App — Map Discovery

**Feature Branch**: `010-mobile-driver-app`

**Created**: 2026-05-26

**Status**: Draft

**Input**: Phase 6 from docs/plan_mvp0.md

## User Scenarios & Testing

### User Story 1 — Driver discovers nearby stations on a map (Priority: P1)

An EV driver opens the mobile app and sees a full-viewport map centered on Tunisia. After granting location permission, the map centers on their current location and nearby charging stations appear as green markers with lightning bolt icons. The driver can pan and zoom the map freely.

**Why this priority**: This is the core value proposition of the mobile app — discovering nearby charging stations spatially. Without this, the app has no purpose.

**Independent Test**: Open the app, grant location permission, and verify that station markers appear on the map within the default 20km radius. Markers disappear when panning far from any stations.

**Acceptance Scenarios**:

1. **Given** a driver opens the app for the first time, **When** the map loads, **Then** a full-viewport map centered on Tunisia is displayed
2. **Given** the driver grants location permission, **When** the location is received, **Then** the map re-centers on the driver's current location
3. **Given** stations exist within the search radius, **When** the map loads, **Then** station markers (green circle with lightning bolt icon) appear on the map
4. **Given** no stations exist within the search radius, **When** the map loads, **Then** the map shows an empty state message indicating no stations found
5. **Given** the driver denies location permission, **When** the app continues, **Then** the map stays centered on Tunisia with a notice that location access was denied

---

### User Story 2 — Driver views station details in a bottom sheet (Priority: P1)

The driver taps a station marker on the map, and a bottom sheet slides up showing the station's name, address, distance, available charger count, and a list of chargers with their types and statuses. The driver can dismiss the sheet by swiping down or tapping the map.

**Why this priority**: Tapping a marker is the primary interaction pattern. Without the detail sheet, the driver cannot make informed decisions about which station to visit.

**Independent Test**: Tap a station marker — a bottom sheet slides up with station info and charger list. Swipe down to dismiss. Tap another marker to see its details.

**Acceptance Scenarios**:

1. **Given** a station marker is visible on the map, **When** the driver taps it, **Then** a bottom sheet slides up displaying station name, address, city, distance, and available charger count
2. **Given** the bottom sheet is open, **When** the driver views the charger list, **Then** each charger shows connector type, power (kW), current type (AC/DC), and status badge
3. **Given** the bottom sheet is open, **When** the driver swipes down on the sheet, **Then** the sheet dismisses and the map is fully visible again
4. **Given** the bottom sheet is open and the driver taps a different marker, **When** the sheet updates, **Then** it shows the newly selected station's details

---

### User Story 3 — Driver navigates to a station (Priority: P2)

The driver can tap a "Navigate" button in the bottom sheet, which opens the device's default maps application with the station's coordinates as the destination.

**Why this priority**: Once the driver finds a station, they need directions to reach it. This completes the core discovery-to-arrival flow.

**Independent Test**: Tap a station marker, tap "Navigate" button in the bottom sheet — device maps app opens with the station coordinates pre-filled.

**Acceptance Scenarios**:

1. **Given** the driver is viewing station details in the bottom sheet, **When** they tap "Navigate", **Then** the device's default maps application opens with the station's coordinates as the destination
2. **Given** the device does not have a maps application, **When** the driver taps "Navigate", **Then** a fallback message is displayed

---

### User Story 4 — Driver adjusts search radius (Priority: P2)

The driver can change the search radius via a slider control (5km / 10km / 20km / 50km). When the radius changes, the nearby stations are re-fetched and markers update accordingly. The driver can also pull down on the map to refresh results.

**Why this priority**: Drivers in rural areas may need a larger radius; drivers in dense urban areas may want a smaller radius for precision.

**Independent Test**: Change radius from 20km to 50km — new station markers appear further away. Pull to refresh — markers re-fetch without app restart.

**Acceptance Scenarios**:

1. **Given** the map is displaying nearby stations, **When** the driver selects a larger radius (e.g., 50km), **Then** additional station markers farther away appear on the map
2. **Given** the map is displaying nearby stations, **When** the driver selects a smaller radius (e.g., 5km), **Then** distant station markers disappear from the map
3. **Given** the driver pulls down on the map, **When** the refresh completes, **Then** station markers update based on the current center and radius

### Edge Cases

- What happens when location permission is denied? The map stays centered on Tunisia with a message explaining that location access is needed for nearby discovery.
- What happens when the device has no internet connection? The map shows cached tiles (if available) with an error message that stations cannot be loaded.
- What happens when the nearby API returns an error? The map shows an error state with a retry option.
- What happens when there are more than 50 stations in range? Only the closest 50 are shown (API hard cap).
- What happens when the driver is outside of Tunisia? The map still shows nearby stations within the search radius; an empty state is displayed if none are found.
- What happens to the bottom sheet if the driver rotates the device? The sheet should adapt to the new orientation without losing its state.

## Requirements

### Functional Requirements

- **FR-001**: Mobile app MUST display a full-viewport map centered on Tunisia [33.8869, 9.5375] zoom 7 on initial load
- **FR-002**: App MUST request device location permission via the operating system's standard permission dialog
- **FR-003**: Upon receiving location, app MUST re-center the map on the user's current location
- **FR-004**: App MUST fetch nearby stations from `/api/v1/stations/nearby` with the user's coordinates and selected radius
- **FR-004a**: The `/api/v1/stations/nearby` endpoint MUST accept unauthenticated (anonymous) requests for the mobile discovery use case
- **FR-005**: Station markers MUST appear as green circles with lightning bolt SVG icons matching the design system
- **FR-006**: Tapping a station marker MUST trigger haptic feedback and open a bottom sheet
- **FR-007**: Bottom sheet MUST display station name, address, city, distance from user, and available charger count
- **FR-008**: Bottom sheet MUST list each charger at the station with connector type, power (kW), current type, and status badge
- **FR-009**: Bottom sheet MUST include a "Navigate" button that opens the device's default maps app with station coordinates
- **FR-010**: Search radius MUST default to 20km with option to change to 5km, 10km, 20km, or 50km
- **FR-011**: Changing the radius MUST trigger a re-fetch of nearby stations with the new radius
- **FR-012**: Pull-to-refresh on the map MUST re-fetch nearby stations using current map center and selected radius
- **FR-013**: Records marked `is_test = true` MUST be excluded from all mobile discovery results
- **FR-014**: API pagination MUST respect the LIMIT 50 hard cap — no more than 50 results displayed
- **FR-015**: All API errors MUST display a user-friendly error message with a retry option, without crashing
- **FR-016**: Location permission denial MUST show a clear explanation and fall back to the default Tunisia-centered view
- **FR-017**: If more than 20 results are returned, markers MUST be clustered to prevent map clutter

### Key Entities

- **Station**: A physical charging location with name, address, city, coordinates (lng/lat), operational status, and available charger count. Returned by the nearby API.
- **Charger**: An individual charging unit at a station with connector type, power (kW), current type (AC/DC), and status (available/occupied/faulted/offline). Listed in the station detail sheet.
- **Driver Location**: The user's current device coordinates (lng/lat), obtained via device GPS. Determines the center point for nearby search and distance calculations.

## Success Criteria

### Measurable Outcomes

- **SC-001**: A driver can open the app, grant location, and see nearby stations within 5 seconds on a standard mobile connection
- **SC-002**: Tapping a station marker shows the bottom sheet in under 500 milliseconds
- **SC-003**: Changing the search radius re-fetches and re-renders markers within 3 seconds
- **SC-004**: The app never crashes or shows a blank white screen under any error condition (no internet, API down, permission denied)
- **SC-005**: Drivers can complete the full flow (open → discover → view details → navigate) in under 30 seconds
- **SC-006**: Test stations (`is_test = true`) never appear in any mobile discovery result

## Clarifications

### Session 2026-05-26

- Q: How does the mobile app authenticate for the nearby API? → A: The nearby endpoint is made publicly accessible (no JWT required). Anonymous discovery is read-only and poses no data isolation risk.
- Q: What is explicitly out of scope for this phase? → A: User accounts/registration, saved/bookmarked stations, charging session initiation or payment, station reviews, real-time charger availability polling, push notifications.
- Q: Should the mobile app support multiple languages? → A: English only for MVP0. Localization can be added post-MVP.

## Out of Scope

The following are explicitly excluded from this phase:

- User registration, login, or account management — the app works anonymously
- Saved/bookmarked stations or favorites
- Charging session initiation or payment processing
- Station reviews or ratings (`REV-` prefix reserved but unused)
- Real-time charger availability polling (deferred post-MVP)
- Push notifications
- Offline maps or station caching

## Assumptions

- The `/api/v1/stations/nearby` endpoint is already implemented (Phase 2 completed) and functional
- The mobile app targets iOS and Android via Expo Go (managed workflow) — no native code
- Device GPS provides accurate-enough coordinates for nearby search (typical accuracy 10-50m outdoors)
- The backend API is deployed and accessible from mobile devices (no localhost requirement)
- Haptic feedback is a "nice to have" enhancement — apps without haptic support degrade gracefully
- Marker clustering is only needed when results exceed 20; below that, individual markers render fine
- The bottom sheet library handles swipe-to-dismiss and orientation changes natively
- Station charger count is computed by the backend and included in the nearby API response
