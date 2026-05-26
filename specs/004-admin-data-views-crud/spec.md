# Feature Specification: Admin Data Views & CRUD

**Feature Branch**: `008-admin-data-views-crud`

**Created**: 2026-05-26

**Status**: Draft

**Input**: User description: "Admin Portal — Data Views & CRUD (Phase 4 from plan_mvp0.md)"

## Clarifications

### Session 2026-05-26

- Q: Charger list display — nested under station detail, flat list, or both? → A: Both — chargers accessible via station detail page AND a flat /data/chargers list with station filter dropdown
- Q: Create/edit form pattern — modal dialogs, dedicated pages, or inline editing? → A: Modal dialogs for all create/edit forms

## User Scenarios & Testing

### User Story 1 — Partners Registry Management (Priority: P1)

An admin opens the Data section, navigates to Partners, and sees a scrollable table listing all partner profiles. The admin can create a new partner by filling a form with user details and partner profile fields; the form dynamically shows or hides the Tax ID field based on the classification toggle (Business vs Private). The admin can edit existing partners via a modal form. When deleting a partner, a confirmation modal requires typing the exact `PRT-` ID before the delete button enables.

**Why this priority**: Partners are the foundational entity — stations and chargers depend on them. Without partner management, the downstream data sections cannot operate.

**Independent Test**: Can be fully tested by navigating to Data → Partners, creating a partner with Business classification, verifying Tax ID field appears, creating a Private partner (Tax ID hidden), editing both, and deleting one via exact ID confirmation.

**Acceptance Scenarios**:

1. **Given** the admin is on the Data → Partners page, **When** they view the partner list, **Then** a scrollable table displays columns: ID, Display Name, Classification, Tax ID, Contact Phone, Created date.
2. **Given** the admin clicks "Create Partner", **When** they toggle classification to "Business", **Then** the Tax ID field becomes visible; when toggled to "Private", the Tax ID field hides.
3. **Given** the admin attempts to delete a partner, **When** the confirmation modal appears, **Then** the confirm button is disabled until the exact `PRT-` ID is typed into the input field.
4. **Given** a partner is deleted, **When** the page refreshes, **Then** the partner no longer appears in the list (soft delete).

---

### User Story 2 — Stations Management (Priority: P1)

An admin navigates to Data → Stations and sees a scrollable table listing all stations with their name, city, owner, coordinates, and operational status. The admin can create a new station with name, address, city, coordinate inputs (lng/lat fields or map picker), owner dropdown populated from partners, and an operational toggle. Clicking a station row on the table pans the BaseMap to that station's location; clicking a station marker on the map highlights the corresponding table row. Deleting a station requires typing the exact `STN-` ID in the confirmation modal.

**Why this priority**: Stations are the core spatial entity. Managing them involves both tabular and map interaction, which is the distinguishing feature of the admin portal.

**Independent Test**: Navigate to Data → Stations, create several stations with different coordinates, verify they appear both in the table and on the map. Click a table row and verify the map pans. Delete a station via ID confirmation.

**Acceptance Scenarios**:

1. **Given** the admin is on the Data → Stations page, **When** they view the station list, **Then** a scrollable table displays columns: ID, Name, City, Owner, Coordinates, Operational status, is_test flag.
2. **Given** the admin creates a station, **When** they select an owner from the dropdown, **Then** the dropdown is populated from the partners API.
3. **Given** the admin clicks a station row in the table, **When** the map is visible, **Then** the map pans and centers on that station's coordinates.
4. **Given** the admin clicks a station marker on the map, **When** the table view is visible, **Then** the corresponding table row is visually highlighted.
5. **Given** the admin deletes a station, **When** the confirmation modal appears, **Then** the button enables only after typing the exact `STN-` ID.

---

### User Story 3 — Chargers Management (Priority: P1)

An admin manages chargers through two complementary views: a flat `/data/chargers` list showing all chargers across all stations (with a station filter dropdown), and a nested view within a station's detail page at `/stations/:id/chargers`. The charger list shows ID, Station name, Connector Type, Power kW, Current Type, and Status (with color-coded badges). The admin can create a charger by selecting a station, choosing a connector type from a dynamically populated dropdown, and setting power, current type, and status. Deleting a charger uses a hard delete with `<ConfirmDeleteModal/>` requiring the `CHG-` ID.

**Why this priority**: Chargers are the leaf entity in the hierarchy and the most frequently updated (status changes). Their connector type dropdown demonstrates the cross-workspace dependency flow.

**Independent Test**: Navigate to `/data/chargers` to see all chargers flat, filter by station, then drill into a station detail page to see only that station's chargers. Create a charger, verify status badge colors, delete via modal.

**Acceptance Scenarios**:

1. **Given** the admin navigates to `/data/chargers`, **When** the page loads, **Then** all chargers across all stations are displayed in a flat table with a station filter dropdown.
2. **Given** the admin selects a station from the filter dropdown, **When** the list updates, **Then** only chargers belonging to that station are shown.
3. **Given** the admin navigates to a station detail page, **When** the chargers section loads, **Then** only chargers for that station are displayed.
4. **Given** the admin views chargers in either view, **When** the list loads, **Then** each charger shows a status badge: green for available, amber for occupied, red for faulted, gray for offline.
5. **Given** the admin creates a charger, **When** they open the connector type dropdown, **Then** the dropdown is dynamically populated from the `station_connector_types` API.
6. **Given** the admin creates a new connector type in Settings, **When** they return to Chargers create form, **Then** the new type appears in the dropdown without a page reload.
7. **Given** the admin deletes a charger, **When** the confirmation modal appears, **Then** the button enables only after typing the exact `CHG-` ID.

---

### User Story 4 — Infrastructure Types Management (Priority: P2)

An admin navigates to Settings → Infrastructure Types and sees a list of connector types. They can create new types with a unique name and description, edit existing ones, and delete unused ones. If a connector type is in use by any charger, deletion is blocked with an error message explaining why.

**Why this priority**: Infrastructure types are a foundational configuration that affects the chargers dropdown. The delete-restrict check prevents data integrity issues.

**Independent Test**: Create a new connector type, verify it appears in the chargers dropdown, attempt to delete a type that is in use and verify the error, delete an unused type.

**Acceptance Scenarios**:

1. **Given** the admin is on Settings → Infrastructure Types, **When** they create a new type, **Then** the type is immediately available in the chargers connector type dropdown.
2. **Given** a connector type is referenced by one or more chargers, **When** the admin attempts to delete it, **Then** an error message is shown and deletion is prevented.
3. **Given** a connector type has no associated chargers, **When** the admin confirms deletion, **Then** the type is soft-deleted and removed from the list.

---

### User Story 5 — App Settings Placeholder (Priority: P3)

An admin navigates to Settings → App and sees placeholder cards for branding, map tokens, and dropzones configuration. These cards are non-functional in MVP0, providing only the structural framework for future implementation.

**Why this priority**: This is a structural placeholder with no functional requirements. It provides the navigation target and visual structure only.

**Independent Test**: Navigate to Settings → App, verify three placeholder cards render without errors.

**Acceptance Scenarios**:

1. **Given** the admin navigates to Settings → App, **When** the page loads, **Then** three placeholder cards (Branding, Map Tokens, Dropzones) are displayed.
2. **Given** the admin interacts with any placeholder card, **When** they click buttons or inputs, **Then** no functional action occurs (structure only).

### Edge Cases

- What happens when the Partners API returns an empty list? The table shows an empty state with a "No partners found" message.
- How does the system handle creating a station without selecting an owner? The owner dropdown defaults to unselected and the form validates that a selection is made before submission.
- What happens when the Connector Types API is unavailable? The charger create form shows a loading error state in the dropdown and allows the admin to retry.
- How does the table handle very long text values (e.g., long names or IDs)? The `<ScrollableTable/>` enforces min-width 800px with horizontal scroll and no word-break, preventing layout breakage.
- What happens when a delete confirmation modal is dismissed without confirming? The modal closes, no action is taken, and the data remains unchanged.
- How does the system handle concurrent edits? MVP0 does not implement optimistic locking; last-write-wins with server-side validation.
- What happens when creating a partner with a duplicate email? The API returns a validation error, displayed inline on the form.

## Requirements

### Functional Requirements

- **FR-001**: System MUST display a scrollable table of all partners on `/data/partners` with columns: ID, Display Name, Classification, Tax ID, Contact Phone, Created date.
- **FR-002**: System MUST allow creating a partner with user fields (email, password, name) and partner profile fields (display_name, classification, tax_id, contact_phone).
- **FR-003**: Classification toggle (Business/Private) MUST conditionally show/hide the Tax ID field.
- **FR-004**: System MUST allow editing partner profiles via a modal form.
- **FR-005**: System MUST require exact `PRT-` ID match in `<ConfirmDeleteModal/>` before enabling the delete button for partners.
- **FR-006**: System MUST display a scrollable table of all stations on `/data/stations` with columns: ID, Name, City, Owner, Coordinates, Operational, is_test.
- **FR-007**: System MUST provide a create station form with name, address, city, coordinates (lng/lat inputs), owner dropdown (populated from partners API), and is_operational toggle.
- **FR-008**: System MUST support bidirectional map-table interaction: clicking a table row pans the BaseMap to the station; clicking a map marker highlights the table row.
- **FR-009**: System MUST require exact `STN-` ID match in `<ConfirmDeleteModal/>` before enabling the delete button for stations.
- **FR-010**: System MUST display chargers in two complementary views: a flat `/data/chargers` list with a station filter dropdown showing all chargers, and a nested view within each station's detail page showing only that station's chargers.
- **FR-018**: System MUST display chargers with color-coded status badges: available (green), occupied (amber), faulted (red), offline (gray).
- **FR-011**: System MUST dynamically populate the connector type dropdown in charger forms from the `station_connector_types` API.
- **FR-012**: System MUST support creating new connector types in Settings → Infrastructure Types with a unique name and description.
- **FR-013**: System MUST prevent deletion of a connector type that is referenced by any charger, showing an error message instead.
- **FR-014**: System MUST require exact `CHG-` ID match in `<ConfirmDeleteModal/>` before enabling the delete button for chargers.
- **FR-015**: System MUST display three non-functional placeholder cards on `/settings/app` for Branding, Map Tokens, and Dropzones.
- **FR-016**: System MUST show an empty state message when any list returns zero results, rather than an empty table.
- **FR-017**: System MUST show inline validation errors on forms when the API returns a validation failure.

### Key Entities

- **Partner Profile**: Represents a partner organization. Contains display_name, classification (Business/Private), tax_id (Business only), contact_phone, logo_url. Linked to a User via owner_id. Referenced by Stations.
- **Station**: A physical charging location with name, address, city, coordinates (PostGIS geometry), is_operational flag. Owned by a Partner. Contains Chargers.
- **Charger**: An individual charging unit at a Station. Has connector_type, power_kw, current_type (AC/DC), status (available/occupied/faulted/offline). Hard-deleted (not soft delete).
- **Connector Type** (station_connector_types): A reusable configuration entity with unique name and description. Referenced by Chargers. Delete-restricted when in use.

## Success Criteria

### Measurable Outcomes

- **SC-001**: An admin can complete the full partner create → edit → delete cycle in under 3 minutes.
- **SC-002**: Admin can create a station with coordinates and verify its marker appears on the map within 5 seconds of submission.
- **SC-003**: Connector type changes in Settings are reflected in the Chargers dropdown within 5 seconds without a page reload.
- **SC-004**: The confirmation modal accurately prevents deletion for 100% of attempts where the typed ID does not match the resource ID.
- **SC-005**: All data tables prevent horizontal layout breakage at any viewport width ≥ 800px.
- **SC-006**: Empty API responses render a user-friendly empty state message rather than a blank or broken UI.

## Assumptions

- The backend CRUD API endpoints from Phase 1 are fully implemented and available (users, partners, stations, chargers, connector-types).
- Authentication scaffold from Phase 1 is functional; all API requests include a valid JWT.
- The admin user has full CRUD permissions on all entity types (no role-based restrictions within the admin portal for MVP0).
- The BaseMap component from Phase 3 is available and supports the pan-to-coordinates and highlight-row APIs needed for bidirectional interaction.
- The `<ScrollableTable/>`, `<SettingsCard/>`, `<SelectSetting/>`, and `<ConfirmDeleteModal/>` components from Phase 3 are available in `@bornemap/ui`.
- The connector type dropdown in charger forms is populated client-side from a cached/enumerated list that refreshes when new types are created in Settings.
- Soft delete applies to partners, stations, and connector types; hard delete applies to chargers.
- The `CONN-` type prefix is a typo in the original spec; the actual prefix used is `CNT-` as defined in the database schema (Constitution Principle II: prefix `CNT-` for connector types).
