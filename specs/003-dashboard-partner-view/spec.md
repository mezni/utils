# Feature Specification: Dashboard Partner View

**Feature Branch**: `003-dashboard-partner-view`

**Created**: 2026-06-09

**Status**: Draft

**Input**: User description: "Sprint 1.3 — Dashboard Partner View"

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Partner Overview Screen (Priority: P1)

A partner opens the Dashboard App with the dev role switcher set to Partner View and a specific partner selected. The Overview screen shows three stat cards (own stations count, own chargers count, available chargers count), a status bar reflecting the partner's current flags (Verified / Awaiting Verification, Live / Not Live, Active / Suspended), and a table of the partner's own stations with name, charger count, and availability status.

**Why this priority**: The Overview is the default landing screen for the partner view. It gives the partner immediate insight into their operational status and station fleet. It depends on the AppShell and RoleContext from Sprint 1.2 but has no other internal dependencies.

**Independent Test**: Set dev role switcher to Partner View, select PRT001. Overview loads with stat cards showing counts for PRT001's data only. Status bar shows Verified, Live, Active badges. Stations table shows only stations belonging to PRT001. Switch to PRT002 — Overview updates to show PRT002's data (different stats, "Not Live" status bar).

**Acceptance Scenarios**:

1. **Given** Partner View is active with PRT001 selected, **When** the Overview screen loads, **Then** stat cards show the count of stations belonging to PRT001, the count of chargers belonging to PRT001's stations, and the count of available chargers among them.
2. **Given** the Overview screen is loaded, **When** the partner's is_verified flag is true, **Then** the status bar shows "Verified" in green.
3. **Given** the Overview screen is loaded, **When** the partner's is_verified flag is false, **Then** the status bar shows "Awaiting Verification" in gray.
4. **Given** the Overview screen is loaded, **When** the partner's is_live flag is true, **Then** the status bar shows "Live" in green.
5. **Given** the Overview screen is loaded, **When** the partner's is_live flag is false, **Then** the status bar shows "Not Live" in gray.
6. **Given** the Overview screen is loaded, **When** the partner's is_active flag is true, **Then** the status bar shows "Active" in green.
7. **Given** the Overview screen is loaded, **When** the partner's is_active flag is false, **Then** the status bar shows "Suspended" in red.
8. **Given** the Overview screen is loaded, **When** the partner has stations, **Then** the own stations table lists each station with name, charger count, and availability status (from station_availability).
9. **Given** the Overview screen is loaded, **When** the mock API is unreachable, **Then** an ErrorState with a Retry button is shown.

---

### User Story 2 — Partner My Stations Screen (Priority: P1)

A partner opens the My Stations screen. A data table shows only stations belonging to the selected partner. The partner can add a new station (with partner_id pre-filled and locked), edit their own station, and delete their own station.

**Why this priority**: Station management is the core operational task for a partner. Partners need to add and update their station information without seeing other partners' data. This screen builds on the Stations infrastructure from Sprint 1.2 (US4) but scopes everything to the selected partner.

**Independent Test**: Set Partner View to PRT001. My Stations shows only PRT001's stations. Add a station — partner_id is locked to PRT001. Edit a station — changes save. Delete a station — confirmation and removal. Switch to PRT002 — different station set.

**Acceptance Scenarios**:

1. **Given** the My Stations screen is loaded, **When** the partner is PRT001, **Then** the data table shows only stations where partner_id matches PRT001.
2. **Given** the My Stations screen is loaded, **When** the partner clicks "Add Station", **Then** a modal opens with partner_id pre-filled and locked (not editable).
3. **Given** the Add Station modal is open, **When** the partner submits valid data, **Then** a new station is created with the partner's partner_id and appears in the table.
4. **Given** a station row, **When** the partner clicks Edit, **Then** a modal opens pre-filled with current values, and submitting updates the station.
5. **Given** a station row, **When** the partner clicks Delete, **Then** a confirmation modal appears and confirming removes the station.
6. **Given** the My Stations screen, **When** the partner has no stations, **Then** an EmptyState with "Add your first station" prompt is shown.
7. **Given** the My Stations screen, **When** the API is unreachable, **Then** an ErrorState with Retry is shown.

---

### User Story 3 — Partner My Chargers Screen (Priority: P2)

A partner opens the My Chargers screen. A data table shows only chargers belonging to the partner's stations. A station filter dropdown is scoped to the partner's own stations only. The partner can add chargers (station select shows only own stations), edit chargers, and delete chargers.

**Why this priority**: Chargers are the operational unit that partners need to manage day to day — marking maintenance, adding new chargers, removing old ones. This screen depends on the My Stations screen (US2) for station data and the Chargers infrastructure from Sprint 1.2 (US5).

**Independent Test**: Open My Chargers with PRT001 selected. Table shows only chargers belonging to PRT001's stations. Station filter shows only PRT001's stations. Add a charger — station select shows only PRT001 stations. Edit changes save. Delete removes. Switch to PRT002 — different charger set.

**Acceptance Scenarios**:

1. **Given** the My Chargers screen is loaded with PRT001, **When** the data loads, **Then** the table shows only chargers belonging to stations where partner_id equals PRT001.
2. **Given** the My Chargers screen, **When** the partner selects a station from the filter dropdown, **Then** the dropdown lists only the partner's own stations.
3. **Given** the My Chargers screen, **When** the partner clicks "Add Charger", **Then** the station select dropdown lists only the partner's own stations.
4. **Given** a charger row, **When** the partner clicks Edit, **Then** a modal opens with pre-filled values, and submitting updates the charger.
5. **Given** a charger row, **When** the partner clicks Delete, **Then** a confirmation modal appears and confirming removes the charger.
6. **Given** the My Chargers screen, **When** no chargers exist for the partner, **Then** an EmptyState is shown.
7. **Given** the My Chargers screen, **When** the API is unreachable, **Then** an ErrorState with Retry is shown.

---

### User Story 4 — Partner Availability Screen (Priority: P2)

A partner opens the Availability screen. A table lists the partner's own stations with their current availability status. An inline three-option toggle per station (Available / Partial / Unavailable) allows the partner to update a station's availability. Selecting a new status POSTs a new record to the station_availability resource. The current status shown is the latest entry per station.

**Why this priority**: Availability management directly impacts driver experience. A partner must be able to quickly mark a station as partially available or unavailable when chargers are down. This is a lightweight update mechanism that doesn't require editing individual chargers.

**Independent Test**: Open Availability with PRT001. Table shows PRT001's stations with current availability. Toggle a station from Available to Unavailable — the status updates immediately. Fetch station_availability from the API confirms a new record was created.

**Acceptance Scenarios**:

1. **Given** the Availability screen is loaded with PRT001, **Then** the table lists each of PRT001's stations with its current availability status (the latest station_availability record for that station).
2. **Given** a station row, **When** the partner clicks the "Unavailable" toggle, **Then** a new station_availability record is created with status="unavailable" and the table updates to show Unavailable.
3. **Given** a station row, **When** the partner clicks the "Partial" toggle, **Then** a new record is created with status="partial".
4. **Given** a station row, **When** the partner clicks the "Available" toggle, **Then** a new record is created with status="available".
5. **Given** the Availability screen, **When** the station has no existing availability records, **Then** the current status defaults to "Unknown" and all three toggle options are available.
6. **Given** the Availability screen, **When** the partner has no stations, **Then** an EmptyState is shown.
7. **Given** the Availability screen, **When** the API is unreachable, **Then** an ErrorState with Retry is shown.
8. **Given** the Availability screen, **When** a toggle action fails due to network error, **Then** an error message is shown and the status reverts to its previous value.

---

### Edge Cases

- Partner View is active but no partner is selected from the dropdown — all screens show a prompt to select a partner
- Partner has zero stations — My Stations and Availability show EmptyState; My Chargers header still works but table is empty
- A partner's station has multiple availability records — the table shows only the latest record per station (by updated_at)
- Partner clicks the same status that is currently active — no new record is created (the toggle is disabled or ignored)
- Partners PRT001 (verified+live+active), PRT002 (verified+active+not live), PRT003 (not verified+active+not live) produce different status bars on Overview
- The partner selector in the dev role switcher shows all partners including deleted ones (json-server soft return) — the partner should select an active partner
- A partner's station is deleted by an admin — the station disappears from the partner's My Stations and Availability tables on the next fetch

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The Partner Overview screen MUST display three stat cards: own stations count, own chargers count, and available chargers count — all scoped to the selected partner.
- **FR-002**: The Partner Overview screen MUST display a status bar showing three flags: verified status (Verified / Awaiting Verification), live status (Live / Not Live), and active status (Active / Suspended) — driven by the selected partner's is_verified, is_live, and is_active fields.
- **FR-003**: The status bar flags MUST use green for true/positive states, gray for neutral states (not verified, not live), and red for suspended.
- **FR-004**: The Partner Overview screen MUST display a table of the partner's own stations with columns: station name, charger count, and current availability status.
- **FR-005**: The My Stations screen MUST display a data table scoped to stations where partner_id matches the selected partner.
- **FR-006**: The Add Station modal in My Stations MUST have the partner_id field pre-filled and locked to the selected partner's ID.
- **FR-007**: The My Stations screen MUST support Edit and Delete actions on the partner's own stations.
- **FR-008**: The My Chargers screen MUST display a data table scoped to chargers belonging to the selected partner's stations.
- **FR-009**: The My Chargers screen MUST have a station filter dropdown that lists only the selected partner's stations.
- **FR-010**: The Add Charger modal's station select dropdown MUST list only the selected partner's stations.
- **FR-011**: The My Chargers screen MUST support Edit and Delete actions on the partner's own chargers.
- **FR-012**: The Availability screen MUST list the partner's own stations with the current availability status (latest station_availability record per station).
- **FR-013**: The Availability screen MUST provide a three-option inline toggle per station: Available, Partial, Unavailable.
- **FR-014**: Selecting a new availability status MUST POST a new record to the station_availability resource with the station_id, selected status, and a timestamp.
- **FR-015**: The Availability screen MUST NOT create a new record if the partner selects the same status that is already active.
- **FR-016**: When Partner View is active but no partner is selected, ALL partner screens MUST show a message prompting the user to select a partner from the dropdown.
- **FR-017**: Every partner screen MUST show ErrorState with a Retry button when the API is unreachable.
- **FR-018**: Every partner screen with zero records MUST show EmptyState with an appropriate create prompt.
- **FR-019**: The partner scope (partner_id) MUST be read from the dev role switcher's RoleContext (selectedPartnerId). The partner cannot see or interact with any data belonging to another partner.

### Key Entities *(include if feature involves data)*

- **Partner**: Organization or individual operating EV charging stations. Attributes: id, name, type, is_verified, is_live, is_active. Each partner has its own set of stations and chargers. Partner flags determine visibility and capabilities.
- **Station**: A physical location with charging equipment. Belongs to exactly one Partner. Each station has multiple chargers and an availability status.
- **Charger**: An individual charging unit at a station. Belongs to exactly one Station. Status can be available, in_use, maintenance, or offline.
- **Station Availability**: Append-only log of station availability status changes. Attributes: id, station_id, status (available/partial/unavailable), updated_by, updated_at. The current status is the row with the latest updated_at per station.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A partner can view their operational status at a glance — the status bar correctly reflects all three partner flags without a page reload when switching between partners.
- **SC-002**: A partner sees exactly their own stations in My Stations and no other partner's stations — verified by switching between PRT001 and PRT002 and confirming the station lists are different and non-overlapping.
- **SC-003**: A partner can add a station in under 30 seconds — partner_id is pre-filled and cannot be changed, eliminating scope errors.
- **SC-004**: A partner can update a station's availability with a single click — the status reflects immediately and is confirmed by fetching the station_availability resource.
- **SC-005**: A partner using the My Chargers screen sees only their own chargers — verified by cross-referencing charger table rows against the partner's station list.
- **SC-006**: All four partner screens handle API downtime gracefully — ErrorState with Retry is shown on every screen, and clicking Retry recovers when the API is back.
- **SC-007**: A partner cannot reach or see any data belonging to a different partner through any of the four partner screens — verified by switching the selected partner and confirming data scope changes.

## Assumptions

- The dev role switcher and RoleContext from Sprint 1.2 are complete — selectedPartnerId is available to all partner screens.
- The AppShell, Sidebar, and navigation from Sprint 1.2 are complete — the Partner View navigation items (Overview, My Stations, My Chargers, Availability) are already wired in the Sidebar component.
- The mock API from Sprint 1.1 serves all four resources with filter queries (station_availability filterable by station_id).
- The selectedPartnerId from RoleContext matches the partner's `id` field in the mock API (e.g., "PRT001").
- Partners cannot create, update, or delete other partners — this sprint only applies data-scoping rules in the UI. Full auth enforcement arrives in MVP-3.
- The availability toggle creates new station_availability records (append-only) — records are never updated in place.
- A partner with is_verified=false or is_active=false can still access the Dashboard and see their data. Restrictions on login come with MVP-3 authentication.
