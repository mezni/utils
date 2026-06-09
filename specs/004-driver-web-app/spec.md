# Feature Specification: Driver Web App

**Feature Branch**: `004-driver-web-app`

**Created**: 2026-06-09

**Status**: Draft

**Input**: Sprint 1.4 — Driver Web App shows a Leaflet map with real station markers from json-server and navigates to station detail.

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Map Discovery (Priority: P1)

A driver opens the Driver Web App and sees a full-screen Leaflet map centered on Tunisia with color-coded markers for each station belonging to verified, live, and active partners. Stations with at least one available charger show a green marker; stations with zero available chargers show a red marker. Clicking a marker shows a popup with the station name, address, available/total charger count, and a link to view details.

**Why this priority**: The map screen is the primary entry point for all drivers. Without it, there is no way to discover nearby stations. It replaces the current reliance on the Dashboard for station discovery.

**Independent Test**: Open the Driver Web App at its URL. The page loads a full-screen Leaflet map centered on Tunisia. Station markers appear for partners PRT001 and PRT002 (both verified, live, and active). No markers appear for PRT003 (is_verified: false). Stations with chargers in "available" status show green markers; stations with zero available chargers show red markers. Click a marker — popup shows name, address, charger count, and a "View Details" link.

**Acceptance Scenarios**:

1. **Given** the Driver Web App loads, **When** the map initializes, **Then** it displays a Leaflet map with OpenStreetMap tiles centered on Tunisia (approximate center 33.89°N, 9.54°E) at zoom level 7.
2. **Given** the map is loaded, **When** station data arrives from the API, **Then** markers appear only for stations where the owning partner has is_verified=true AND is_live=true AND is_active=true.
3. **Given** the map shows a station with at least one available charger, **Then** its marker displays a green fill color.
4. **Given** the map shows a station with zero available chargers, **Then** its marker displays a red fill color.
5. **Given** a marker is visible on the map, **When** the driver clicks it, **Then** a popup displays the station name, address, charger availability count (e.g., "3/5 available"), and a "View Details" link.
6. **Given** the API is unreachable, **When** the map screen loads, **Then** an error message is displayed with a retry option.
7. **Given** the driver is viewing the map, **When** they scroll or pan, **Then** the map responds with smooth panning and zooming appropriate for a Leaflet map.

---

### User Story 2 — Station Detail (Priority: P2)

A driver clicks the "View Details" link on a station's popup and navigates to a dedicated Station Detail screen. This screen shows the station's full name and address, along with a list of its chargers showing each charger's connector type, power rating, and current status.

**Why this priority**: Detail is necessary for a driver to decide which station to visit. However, the map with availability count already gives sufficient info for initial discovery. Detail is a secondary navigation.

**Independent Test**: Open map, click any station marker, click "View Details" in the popup. The Station Detail screen loads showing the station name, address, and a list of all chargers at that station. Each charger shows connector type, power in kW, and a status badge.

**Acceptance Scenarios**:

1. **Given** the driver is on the map screen, **When** they click "View Details" on a station popup, **Then** they navigate to a Station Detail screen for that station.
2. **Given** the Station Detail screen loads, **When** data arrives from the API, **Then** the screen displays the station name and address at the top.
3. **Given** the Station Detail screen loads, **When** charger data arrives, **Then** each charger is displayed with its connector type, power rating in kW, and current operational status.
4. **Given** the Station Detail screen, **When** the driver clicks a back button or navigates back, **Then** they return to the map screen at the same position and zoom level.
5. **Given** the Station Detail screen, **When** the API is unreachable, **Then** an error state is shown with a retry option.
6. **Given** the Station Detail screen is loading data, **Then** a loading skeleton or spinner is shown while content is being fetched.

---

### Edge Cases

- Partner PRT003 (is_verified: false) — its stations must NOT appear on the map at all.
- A station with no chargers — appears on the map with a red marker (zero available), and its detail screen shows an empty charger list with a "No chargers" message.
- All chargers at a station are in "maintenance" or "offline" status — marker is red (zero available).
- A station belonging to a partner that is later deactivated (is_active set to false) — the station disappears from the map on the next data fetch.
- The map viewport is in a region with no stations — the map shows empty terrain with no markers, no crash.
- API returns a non-JSON response or network error — the map shows an error state with retry; no crash or blank white page.
- The driver refreshes the Station Detail page directly (deep link) — the detail screen loads correctly for that station.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The Driver Web App MUST display a full-screen Leaflet map centered on Tunisia at zoom level 7 on initial load.
- **FR-002**: The map MUST use OpenStreetMap tile layers as the base map.
- **FR-003**: The app MUST fetch all stations and chargers from the API on mount.
- **FR-004**: The app MUST filter out stations where the owning partner has is_verified=false OR is_live=false OR is_active=false.
- **FR-005**: Stations with at least one charger in "available" status MUST display a green marker.
- **FR-006**: Stations with zero chargers in "available" status MUST display a red marker.
- **FR-007**: Clicking a station marker MUST open a popup showing: station name, address, available charger count / total charger count, and a "View Details" link.
- **FR-008**: The "View Details" link MUST navigate to a Station Detail screen for that specific station.
- **FR-009**: The Station Detail screen MUST display the station name and address.
- **FR-010**: The Station Detail screen MUST fetch and display all chargers for that station, each showing connector type, power rating in kW, and current status.
- **FR-011**: The Station Detail screen MUST have a back navigation control that returns to the map at the same position and zoom level.
- **FR-012**: The app MUST display a branded top bar with the name "BorneMap".
- **FR-013**: Both screens MUST display a loading state while fetching data.
- **FR-014**: Both screens MUST display an error state with a retry option when the API is unreachable.
- **FR-015**: The app MUST reload data when returning from Station Detail to the map screen, to reflect any status changes made in the Dashboard.

### Key Entities

- **Station**: A physical location with EV charging equipment. Has a name, address, latitude, longitude, and belongs to exactly one Partner. Each station has zero or more chargers. Has a computed available_count derived from its chargers.
- **Charger**: An individual charging unit at a station. Has a connector type, power rating in kW, and an operational status (available, in_use, maintenance, offline).
- **Partner**: The organization that owns stations. Has three operational flags (is_verified, is_live, is_active) that determine whether stations appear on the map. A station is only visible when all three flags are true.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A driver can open the Driver Web App and see station markers on the map within 3 seconds on a standard broadband connection.
- **SC-002**: Only stations belonging to partners with all three flags enabled (is_verified=true, is_live=true, is_active=true) appear on the map — verified by checking that all three partners' stations are visible or hidden according to their flag state.
- **SC-003**: A driver can identify station availability at a glance without clicking — green vs red markers immediately communicate whether a station has working chargers.
- **SC-004**: A driver can navigate from the map to station detail and back within two clicks, without losing map position.
- **SC-005**: Both screens handle API downtime gracefully — an error state with retry is shown, and clicking retry recovers when the API is available again.
- **SC-006**: A charger status change made in the Dashboard (e.g., setting a charger to "maintenance") is reflected in the Driver Web App's marker colors on the next page load.

## Assumptions

- The mock API (json-server) is already running and serves all four resources with filter queries, including the ability to fetch stations, chargers, and partners independently.
- The Leaflet library (including react-leaflet) is available and compatible with Vite + React.
- The Driver Web App runs on modern browsers (Chrome, Firefox, Safari) on desktop and mobile devices.
- GPS / device location is out of scope for Sprint 1.4 — the map uses a fixed Tunisia center without user geolocation.
- The partner visibility rule (is_verified AND is_live AND is_active) is computed client-side by fetching partners and stations separately, then filtering. Full server-side enforcement arrives in MVP-2.
- The haversine distance calculation is used to compute station distance from Tunisia center — this is a visual aid, not a spatial query.
- The map uses calc(100vh - 56px) height to leave room for a floating top bar.
- There is no search, filter, or list view beyond the map markers — those features are out of scope for this sprint.
