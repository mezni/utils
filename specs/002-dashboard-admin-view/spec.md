# Feature Specification: Dashboard Admin View

**Feature Branch**: `002-dashboard-admin-view`

**Created**: 2026-06-09

**Status**: Draft

**Input**: User description: "Sprint 1.2 — Dashboard Admin View"

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Dashboard Shell and Navigation (Priority: P1)

An admin opens the Dashboard App and sees a consistent layout with a fixed sidebar and top bar. The sidebar displays navigation links for Overview, Partners, Stations, and Chargers. A dev role switcher at the bottom of the sidebar allows toggling between Admin View and Partner View. When Partner View is selected, a partner selector dropdown appears.

**Why this priority**: The shell and navigation structure is the foundation that every other screen sits inside. Without it, no feature screen has a home. The dev role switcher is required to support partner view development in the next sprint without authentication.

**Independent Test**: Open the Dashboard App. The left sidebar shows four navigation items (Overview, Partners, Stations, Chargers) with the first item highlighted. The sidebar has a brand header with an icon and "BorneMap" label. Toggle the dev role switcher — the navigation changes to partner items (Overview, My Stations, My Chargers, Availability) and a partner selector dropdown appears.

**Acceptance Scenarios**:

1. **Given** the Dashboard App is open, **When** the page loads, **Then** the user sees a fixed left sidebar (w-64) with a brand header (icon + "BorneMap"), four navigation items (Overview, Partners, Stations, Chargers), a top bar with page title, and a main content area.
2. **Given** the sidebar is visible, **When** the user clicks a navigation item, **Then** that item becomes highlighted (active state) and the main content area shows the corresponding screen.
3. **Given** the sidebar is visible, **When** the user clicks the "Admin View" / "Partner View" toggle at the bottom, **Then** the navigation items switch between admin and partner sets.
4. **Given** Partner View is active, **When** the user opens the partner selector dropdown, **Then** all partners from the mock API are listed and selecting one sets the partner context.
5. **Given** the dev role switcher is visible, **When** the user inspects its label, **Then** it is clearly labeled "Dev Only — removed in MVP-3".
6. **Given** the top bar is visible, **When** the user views any screen, **Then** the current page title is displayed and a placeholder avatar is shown on the right.

---

### User Story 2 — Admin Overview Screen (Priority: P1)

An admin lands on the Overview screen after login. Three stat cards show total partners, total stations, and total chargers. Below is a table listing recent stations with their name, partner name, and charger count.

**Why this priority**: The Overview is the default landing screen. It provides immediate insight into platform scale and recent activity. It depends on the shell (US1) but has no other dependencies.

**Independent Test**: Open the Dashboard App and navigate to Overview. Three stat cards display counts fetched from the mock API. A recent stations table loads with real station data. Stop the mock server — the screen shows an error state with a retry button.

**Acceptance Scenarios**:

1. **Given** the Overview screen is loaded, **When** the mock API returns partner data, **Then** the "Total Partners" stat card shows the correct count from the API.
2. **Given** the Overview screen is loaded, **When** the mock API returns station and charger data, **Then** the "Total Stations" and "Total Chargers" stat cards show correct counts.
3. **Given** the Overview screen is loaded, **When** stations exist, **Then** the recent stations table shows each station's name, partner name, and number of chargers.
4. **Given** the Overview screen is loaded, **When** the mock API is unreachable, **Then** an ErrorState is shown with a "Retry" button.
5. **Given** the Overview screen is loaded, **When** there are no stations in the system, **Then** an EmptyState is shown with a prompt to create the first station.

---

### User Story 3 — Admin Partner Management (Priority: P1)

An admin views a table of all partners showing name, type (business/personal), verification status, live status, active status, and action buttons. The admin can add a new partner, edit a partner, verify a partner, activate/deactivate a partner, and delete a partner.

**Why this priority**: Partner management is the most critical admin capability. Partners are the foundation of the platform — stations belong to partners, and partner flags control visibility. Without this screen, an admin cannot onboard new partners or manage existing ones.

**Independent Test**: Open the Partners screen. The table lists all 3 seeded partners from the mock API. Click "Add Partner" — a modal opens with name and type fields. Submit creates a new partner visible in the table. Click Verify on an unverified partner — the badge updates. Click Deactivate — the active toggle reflects. Delete a partner — the row disappears.

**Acceptance Scenarios**:

1. **Given** the Partners screen is loaded, **When** the mock API returns partners, **Then** each partner row shows name, type badge (Business/Personal), verified badge (green check / gray x), live badge, active toggle, and action buttons.
2. **Given** the Partners screen is loaded, **When** the admin clicks "Add Partner", **Then** a modal opens with a name field and a type select (Business / Personal).
3. **Given** the Add Partner modal is open, **When** the admin submits with valid data, **Then** a new partner is created with is_verified=false, is_live=false, is_active=true and appears in the table.
4. **Given** a partner row with is_verified=false, **When** the admin clicks Verify, **Then** is_verified becomes true, the badge updates, and the UI notes that verifying will also set is_live to true when the partner has stations.
5. **Given** a partner row with is_active=true, **When** the admin clicks Deactivate, **Then** is_active becomes false and the badge updates.
6. **Given** a partner row with is_active=false, **When** the admin clicks Reactivate, **Then** is_active becomes true and the badge updates.
7. **Given** a partner row, **When** the admin clicks Edit, **Then** a modal opens with the current name and type pre-filled, and submitting updates the partner.
8. **Given** a partner row, **When** the admin clicks Delete, **Then** a confirmation modal appears and confirming removes the partner from the table.
9. **Given** the Partners screen is loaded with no partners, **When** the data is empty, **Then** an EmptyState with "Add your first partner" prompt is shown.
10. **Given** the Partners screen is loaded, **When** the API is unreachable, **Then** an ErrorState with Retry is shown.

---

### User Story 4 — Admin Station Management (Priority: P2)

An admin views a table of all stations showing name, address, partner name, charger count, and actions. A dropdown filters stations by partner. The admin can add stations, edit stations, and delete stations.

**Why this priority**: Stations are the second core entity. Admins need to create and manage stations for partners. This screen depends on partners existing (US3) but can be developed in parallel with US5.

**Independent Test**: Open the Stations screen. The table lists all 15 seeded stations. Filter by partner — only that partner's stations show. Add a station — it appears in the table. Edit a station — the row updates. Delete a station — the row disappears.

**Acceptance Scenarios**:

1. **Given** the Stations screen is loaded, **When** the mock API returns stations, **Then** each station row shows name, address, partner name, charger count, and action buttons.
2. **Given** the Stations screen is loaded, **When** the admin selects a partner from the filter dropdown, **Then** only stations belonging to that partner are shown.
3. **Given** the Stations screen is loaded, **When** the admin clicks "Add Station", **Then** a modal opens with fields: name, address, latitude, longitude, and partner select.
4. **Given** the Add Station modal is open, **When** the admin enters latitude outside -90 to 90 or longitude outside -180 to 180, **Then** inline field errors are shown and the form cannot be submitted.
5. **Given** the Stations screen is loaded, **When** the admin clicks Edit on a station, **Then** a modal opens with the current values pre-filled, and submitting updates the station.
6. **Given** the Stations screen is loaded, **When** the admin clicks Delete on a station, **Then** a confirmation modal appears and confirming removes the station.
7. **Given** the Stations screen is loaded, **When** no stations exist, **Then** an EmptyState with "Add your first station" prompt is shown.
8. **Given** the Stations screen is loaded, **When** the API is unreachable, **Then** an ErrorState with Retry is shown.
9. **Given** the Add Station modal is open, **When** required fields (name, partner) are empty, **Then** inline validation errors are shown.

---

### User Story 5 — Admin Charger Management (Priority: P2)

An admin views a table of all chargers showing station name, connector type, power in kW, status badge, and actions. A dropdown filters chargers by station. The admin can add chargers, edit chargers, and delete chargers.

**Why this priority**: Chargers are the smallest operational unit. Admins need to manage them independently. This screen depends on stations existing (US4) but can be developed in parallel with US4.

**Independent Test**: Open the Chargers screen. The table lists all 24 seeded chargers. Filter by station — only that station's chargers show. Add a charger — it appears in the table. Edit a charger to change its status — the status badge updates. Delete a charger — the row disappears.

**Acceptance Scenarios**:

1. **Given** the Chargers screen is loaded, **When** the mock API returns chargers, **Then** each charger row shows station name, connector type (Type 2, CCS, CHAdeMO, Type 1), power in kW, status badge (Available/In Use/Maintenance/Offline with color coding), and action buttons.
2. **Given** the Chargers screen is loaded, **When** the admin selects a station from the filter dropdown, **Then** only chargers belonging to that station are shown.
3. **Given** the Chargers screen is loaded, **When** the admin clicks "Add Charger", **Then** a modal opens with fields: station select, connector type select, power kW input, status select.
4. **Given** the Add Charger modal is open, **When** the admin submits with valid data, **Then** the new charger appears in the table.
5. **Given** the Chargers screen is loaded, **When** the admin clicks Edit on a charger, **Then** a modal opens with current values pre-filled, and submitting updates the charger.
6. **Given** the Chargers screen is loaded, **When** the admin clicks Delete on a charger, **Then** a confirmation modal appears and confirming removes the charger.
7. **Given** the Chargers screen is loaded, **When** no chargers exist, **Then** an EmptyState with "Add your first charger" prompt is shown.
8. **Given** the Chargers screen is loaded, **When** the API is unreachable, **Then** an ErrorState with Retry is shown.

---

### Edge Cases

- Admin opens the app while json-server is not running — all screens show ErrorState with Retry
- Admin opens a screen with zero data — EmptyState with create prompt shown
- Admin submits a form with empty required fields — inline validation errors prevent submission
- Admin enters invalid lat/lng — field-level error messages shown
- Admin verifies a partner that has stations — the UI informs that verifying also sets is_live to true
- Admin deletes a partner — no cascade is performed; stations remain orphaned with the deleted partner's ID
- Admin toggles the dev role switcher while a Partner View screen is active — partner navigation items appear without a partner selected; the partner selector dropdown forces a selection
- Network error occurs mid-form submission — the modal stays open with an error message; data is not lost
- Admin clicks Verify on an already-verified partner — the Verify button is not visible or is disabled
- Admin clicks Deactivate on an already-deactivated partner — only Reactivate is shown

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The system MUST display a fixed left sidebar (w-64) with a brand header, navigation items, and a dev role switcher at the bottom.
- **FR-002**: The sidebar navigation MUST show admin items (Overview, Partners, Stations, Chargers) in Admin View and partner items (Overview, My Stations, My Chargers, Availability) in Partner View.
- **FR-003**: The active navigation item MUST be visually distinct (brand.sageLight background, brand.primary text).
- **FR-004**: The dev role switcher MUST be labeled "Dev Only — removed in MVP-3" and toggle between Admin View and Partner View.
- **FR-005**: The partner selector dropdown MUST appear only when Partner View is active and list all partners from the API.
- **FR-006**: The selected partner in the dev role switcher MUST be stored in React context and available to all partner-view screens.
- **FR-007**: The Overview screen MUST display three stat cards showing total partners, total stations, and total chargers from the mock API.
- **FR-008**: The Overview screen MUST display a table of recent stations showing name, partner name, and charger count.
- **FR-009**: The Partners screen MUST display a data table with columns: name, type, verified badge, live badge, active toggle, actions.
- **FR-010**: The Partners screen MUST support Add Partner via a modal with name field and type select (Business / Personal).
- **FR-011**: New partners MUST be created with is_verified=false, is_live=false, is_active=true.
- **FR-012**: The Verify action MUST set is_verified=true and update the badge immediately. When the partner has stations, the UI MUST note that is_live will also be set to true.
- **FR-013**: The Deactivate/Reactivate action MUST toggle is_active and update the badge immediately.
- **FR-014**: The Edit Partner action MUST open a modal pre-filled with current name and type, and submit updates the partner.
- **FR-015**: The Delete Partner action MUST show a confirmation modal before deleting.
- **FR-016**: The Stations screen MUST display a data table with columns: name, address, partner name, charger count, actions.
- **FR-017**: The Stations screen MUST have a partner filter dropdown that filters stations by partner_id.
- **FR-018**: The Add Station modal MUST include fields: name (required), address, latitude (required, validated -90 to 90), longitude (required, validated -180 to 180), partner select (required).
- **FR-019**: Invalid latitude or longitude values MUST show inline field errors and block form submission.
- **FR-020**: The Chargers screen MUST display a data table with columns: station name, connector type, power kW, status badge, actions.
- **FR-021**: The Chargers screen MUST have a station filter dropdown that filters chargers by station_id.
- **FR-022**: The Add Charger modal MUST include fields: station select (required), connector type select (required), power kW input (required, positive number), status select (required).
- **FR-023**: Every data table MUST show EmptyState when the API returns zero records.
- **FR-024**: Every screen MUST show ErrorState with a Retry button when the API is unreachable or returns an error.
- **FR-025**: All form modals MUST validate required fields before submission and show inline error messages for empty required fields.
- **FR-026**: The dev role switcher state MUST persist across navigation within a session but MUST reset on page reload.
- **FR-027**: The Dashboard App MUST fetch data from the mock API at http://localhost:3001 with the /api prefix on all endpoints.

### Key Entities *(include if feature involves data)*

- **Partner**: Organization or individual operating EV charging stations. Attributes: name, type (business/personal), is_verified, is_live, is_active. Partners own stations.
- **Station**: A physical location with charging equipment. Attributes: name, address, latitude, longitude. Belongs to one Partner. Has multiple Chargers.
- **Charger**: An individual charging unit at a station. Attributes: connector_type (type2/ccs/chademo/type1), power_kw, status (available/in_use/maintenance/offline). Belongs to one Station.
- **Station Availability**: A status record for a station (available/partial/unavailable). Append-only log; current status is the most recent entry.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Admin can create a partner with type set (business/personal) in under 30 seconds from opening the Partners screen.
- **SC-002**: Admin can verify an unverified partner with a single click — the verified badge reflects the change immediately without a page reload.
- **SC-003**: Admin can deactivate and reactivate a partner with a single click — the active toggle reflects the change immediately without a page reload.
- **SC-004**: Admin can add a station with valid lat/lng in under 45 seconds — lat/lng validation rejects values outside valid ranges with inline error messages.
- **SC-005**: Admin can add a charger to an existing station in under 30 seconds — station filter shows only relevant options.
- **SC-006**: All five admin screens (Overview, Partners, Stations, Chargers) show error states when the mock API is stopped — a retry button recovers the screen when the API restarts.
- **SC-007**: Every data screen shows a meaningful empty state when the system has no data — the empty state guides the admin toward the next action.
- **SC-008**: An admin using the Dashboard App for the first time can navigate to all four admin screens, view existing data, and perform all CRUD operations without any training or documentation.

## Assumptions

- The mock API (json-server) is running on localhost:3001 and all endpoints are reachable under the /api prefix as set up in Sprint 1.1.
- No authentication is required — the Dashboard App operates without login in MVP-1.
- The dev role switcher is a development-only feature that will be removed in MVP-3 when Keycloak authentication arrives.
- The three seeded partners from Sprint 1.1 (PRT001 verified+live+active, PRT002 verified+active+not live, PRT003 active+not verified+not live) are available for testing.
- Latitude validation range is -90 to 90, longitude -180 to 180 as defined in the project constitution.
- Deleting a partner does not cascade to stations — orphaned station records remain with the deleted partner's ID.
- Form data is not persisted client-side on network errors — the admin must resubmit after the error resolves.
- The Dashboard App does not need to support offline mode — network connectivity is assumed.
