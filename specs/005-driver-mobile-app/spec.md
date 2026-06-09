# Feature Specification: Driver Mobile App

**Feature Branch**: `005-driver-mobile-app`

**Created**: 2026-06-09

**Status**: Draft

**Input**: Sprint 1.5 — Driver Mobile App shows a map with real station markers from json-server on iOS and Android using React Native + Expo SDK 54.

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Mobile Map Discovery (Priority: P1)

A driver opens the Driver Mobile App on their iOS or Android device and sees a full-screen map centered on their current location (if location permission is granted) or centered on Tunisia (if permission is denied). Station markers appear for stations belonging to verified, live, and active partners. Stations with at least one available charger show a green pin; stations with zero available chargers show a red pin. Tapping a marker shows a callout with the station name and available/total charger count. Tapping the callout navigates to a Station Detail screen.

**Why this priority**: The map screen is the primary entry point for all drivers on mobile. It is the mobile equivalent of Sprint 1.4's Map Discovery screen and must work on both iOS and Android.

**Independent Test**: Install and launch the app on an iOS simulator or Android emulator. The app requests location permission. If granted, the map centers on the device's location. If denied, the map centers on Tunisia. Station markers appear for PRT001 and PRT002 (verified, live, active partners). No markers appear for PRT003. Green pins for stations with available chargers; red pins for stations with zero available chargers. Tap a marker — callout shows station name and charger count. Tap callout — navigates to Station Detail.

**Acceptance Scenarios**:

1. **Given** the Driver Mobile App launches, **When** location permission has not been requested yet, **Then** the app requests foreground location permission.
2. **Given** location permission is granted, **When** the map loads, **Then** it centers on the device's current coordinates.
3. **Given** location permission is denied, **When** the map loads, **Then** it centers on Tunisia (approximately 33.89°N, 9.54°E) without showing an error.
4. **Given** the map is loaded, **When** station data arrives from the API, **Then** markers appear only for stations where the owning partner has is_verified=true AND is_live=true AND is_active=true.
5. **Given** a station has at least one available charger, **Then** its marker displays a green pin color.
6. **Given** a station has zero available chargers, **Then** its marker displays a red pin color.
7. **Given** a marker is visible on the map, **When** the driver taps it, **Then** a callout displays the station name and available/total charger count.
8. **Given** a callout is visible, **When** the driver taps it, **Then** the app navigates to a Station Detail screen for that station.
9. **Given** the API is unreachable, **When** the map loads, **Then** the app shows an error message without crashing.
10. **Given** data is still loading, **Then** an activity indicator is shown while content is being fetched.

---

### User Story 2 — Mobile Station Detail (Priority: P2)

A driver taps a station callout and navigates to a Station Detail screen. This screen shows the station's full name and address, along with a list of its chargers showing each charger's connector type, power rating, and current status as colored text. A back button returns to the map.

**Why this priority**: The detail screen provides the information needed for a driver to decide to visit a station. It is the mobile equivalent of Sprint 1.4's Station Detail screen.

**Independent Test**: Open map, tap a marker, tap the callout. Station Detail loads with station name, address, and charger list. Each charger shows connector type, power, and status. Tap back — returns to map.

**Acceptance Scenarios**:

1. **Given** the driver is on the map screen, **When** they tap a marker's callout, **Then** they navigate to a Station Detail screen for that station.
2. **Given** the Station Detail screen loads, **When** data arrives from the API, **Then** the screen displays the station name and address at the top.
3. **Given** the Station Detail screen loads, **When** charger data arrives, **Then** each charger is displayed with its connector type, power rating in kW, and current operational status as colored text.
4. **Given** the Station Detail screen, **When** the driver taps a back button, **Then** they return to the map screen.
5. **Given** the Station Detail screen, **When** the API is unreachable, **Then** an error message is shown without crashing.
6. **Given** the Station Detail screen is loading data, **Then** an activity indicator is shown while content is being fetched.
7. **Given** a station has no chargers, **Then** a "No chargers" message is shown instead of an empty list.

---

### Edge Cases

- Location permission denied — map uses Tunisia center as fallback without crashing or showing an error message to the user.
- Location permission granted but GPS unavailable — map uses the last known location or Tunisia center as fallback.
- PRT003 (is_verified: false) stations must NOT appear on the map at all.
- A station with no chargers — appears with a red marker and its detail screen shows "No chargers at this station."
- All chargers at a station are in "maintenance" or "offline" status — marker is red (zero available).
- The app is in a region with no stations nearby — the map shows empty terrain with no markers, no crash.
- API returns a non-JSON response or network error — the app shows an error message gracefully, no crash.
- The driver launches Station Detail via a deep link — the screen loads correctly for that station.
- While data is loading, the user navigates away and back — the app handles the lifecycle gracefully without double-fetching or stale state.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The Driver Mobile App MUST request foreground location permission on first launch.
- **FR-002**: If location permission is granted, the map MUST center on the device's current location.
- **FR-003**: If location permission is denied or unavailable, the map MUST center on Tunisia (approximately 33.89°N, 9.54°E) without crashing or showing an error.
- **FR-004**: The app MUST fetch all stations and chargers from the API on mount.
- **FR-005**: The app MUST filter out stations where the owning partner has is_verified=false OR is_live=false OR is_active=false.
- **FR-006**: Stations with at least one charger in "available" status MUST display a green pin marker.
- **FR-007**: Stations with zero chargers in "available" status MUST display a red pin marker.
- **FR-008**: Tapping a station marker MUST show a callout with the station name and available/total charger count.
- **FR-009**: Tapping a callout MUST navigate to the Station Detail screen for that station.
- **FR-010**: The Station Detail screen MUST display the station name and address.
- **FR-011**: The Station Detail screen MUST fetch and display all chargers for that station, each showing connector type, power rating in kW, and current operational status as colored text.
- **FR-012**: The Station Detail screen MUST have a back navigation control that returns to the map.
- **FR-013**: The app MUST display a header with the name "BorneMap" on the map screen.
- **FR-014**: Both screens MUST display an activity indicator while fetching data.
- **FR-015**: Both screens MUST display an error message without crashing when the API is unreachable.
- **FR-016**: The app MUST work on both iOS and Android platforms.

### Key Entities

- **Station**: A physical location with EV charging equipment. Has a name, address, latitude, longitude, and belongs to exactly one Partner. Has a computed available_count derived from its chargers.
- **Charger**: An individual charging unit at a station. Has a connector type, power rating in kW, and an operational status (available, in_use, maintenance, offline).
- **Partner**: The organization that owns stations. Has three operational flags (is_verified, is_live, is_active) that determine whether stations appear on the map. A station is only visible when all three flags are true.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A driver can open the app and see station markers on the map within 5 seconds on a standard mobile connection.
- **SC-002**: Only stations belonging to partners with all three flags enabled appear on the map — verified by checking PRT001/PRT002 stations are present and PRT003 stations are absent on both platforms.
- **SC-003**: A driver can identify station availability at a glance — green vs red pins immediately communicate whether a station has working chargers.
- **SC-004**: A driver can navigate from the map to station detail and back within two taps.
- **SC-005**: The app handles location permission denial gracefully — the user sees a functional map centered on Tunisia without any error messages or crashes.
- **SC-006**: Both screens handle API downtime gracefully — an error message is shown, and the app does not crash.
- **SC-007**: The app functions correctly on both iOS and Android — all features work identically on both platforms.

## Assumptions

- The mock API (json-server) is already running and serves all resources. The mobile app connects to it via the machine's local IP address (since simulators/emulators cannot reach localhost on the host machine directly).
- Location permission is only requested once on first launch. The user can change the permission later in device settings.
- The app does not implement search, filters, or a list view — only the map and detail screens are in scope.
- Authentication is not required — the app is publicly accessible in MVP-1.
- The partner visibility rule is computed client-side (same approach as Driver Web in Sprint 1.4).
