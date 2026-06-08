# Feature Specification: Dashboard App

**Feature Branch**: `002-dashboard-app`

**Created**: June 8, 2026

**Status**: Draft

**Input**: Dashboard App for managing partners, stations, and chargers with real API data

## User Scenarios & Testing

### User Story 1 - Partner Management (Priority: P1)

A Partner Manager needs to create, view, edit, and delete partner records (e.g., charging network companies) through a simple dashboard interface. Partners are the foundation for managing stations and chargers.

**Why this priority**: Partner creation is the first step in the data entry workflow. Without partners, stations and chargers cannot be created. This is the simplest CRUD entity and provides immediate value.

**Independent Test**: Can be fully tested by creating a partner in the dashboard, verifying it appears in the partners list, editing its name, and deleting it. Delivers the value of managing business entities.

**Acceptance Scenarios**:

1. **Given** the Partners screen is open and no partners exist, **When** the user clicks "Create Partner", **Then** a modal opens with a name input field
2. **Given** a modal with a partner name field, **When** the user enters a name and clicks "Save", **Then** the API call succeeds and the new partner appears in the Partners table
3. **Given** the Partners table with multiple partners, **When** the user clicks "Edit" on a partner, **Then** a modal opens with the current partner name pre-filled
4. **Given** a partner edit modal, **When** the user changes the name and clicks "Save", **Then** the table updates immediately without page reload
5. **Given** the Partners table with partners, **When** the user clicks "Delete" and confirms, **Then** the partner is removed from the table and the API is updated
6. **Given** the Partners table is empty, **When** the user views the screen, **Then** an empty state appears with a "Create Partner" prompt

---

### User Story 2 - Station Management (Priority: P1)

A Partner Manager needs to create, view, edit, and delete stations (physical EV charging locations) associated with partners. Each station requires a name, address, and geographic coordinates.

**Why this priority**: Stations are the core business entity. Users must be able to create a station for a partner and see all stations managed by that partner. This directly enables the driver-facing feature.

**Independent Test**: Can be fully tested by creating a station for a partner, verifying it appears with correct partner name and charger count, editing its details, and deleting it. Works independently of chargers.

**Acceptance Scenarios**:

1. **Given** the Stations screen is open, **When** the user clicks "Create Station", **Then** a form appears with fields for name, address, latitude, longitude, and partner selection
2. **Given** a station creation form, **When** the user fills all required fields and clicks "Save", **Then** the API creates the station and it appears in the table with the correct partner name
3. **Given** the Stations table, **When** a partner filter dropdown exists, **Then** the dropdown shows all available partners from the API
4. **Given** a partner is selected in the filter dropdown, **When** the user applies the filter, **Then** only stations belonging to that partner are shown
5. **Given** a station in the table with charger count of 0, **When** the user edits the station's address and saves, **Then** the table updates without reload
6. **Given** the station creation form, **When** the user enters latitude 91 (out of range), **Then** an error message appears and the form cannot be submitted
7. **Given** the station creation form with longitude -181 (out of range), **When** the user attempts to submit, **Then** validation prevents submission with clear error text

---

### User Story 3 - Charger Management (Priority: P1)

A Partner Manager needs to create, view, edit, and delete chargers (individual charging units) associated with stations. Chargers have a connector type, power rating, and operational status (available, in_use, maintenance).

**Why this priority**: Chargers are the atomic unit that drivers interact with. Status updates on chargers directly affect what drivers see on the map. Full charger CRUD is required for the end-to-end loop.

**Independent Test**: Can be fully tested by creating a charger for a station, updating its status, and verifying the status badge reflects the change. Demonstrates full charger lifecycle.

**Acceptance Scenarios**:

1. **Given** the Chargers screen is open, **When** the user clicks "Create Charger", **Then** a form appears with fields for station selection, connector type, power kW, and status
2. **Given** a charger creation form, **When** the user fills all fields and clicks "Save", **Then** the charger appears in the table with the correct station name and a status badge
3. **Given** a charger in the table with status "available" (green badge), **When** the user clicks "Edit" and changes status to "maintenance", **Then** the badge color changes to red after save
4. **Given** a charger in the table, **When** the user clicks "Delete" and confirms, **Then** the charger is removed and the station's charger count decreases by 1
5. **Given** the Chargers screen, **When** a station filter dropdown exists, **Then** the dropdown is populated from the API with all stations
6. **Given** a station is selected in the charger filter, **When** the user applies the filter, **Then** only chargers for that station are shown

---

### User Story 4 - Overview Dashboard (Priority: P2)

A Partner Manager needs to see at a glance how many partners, stations, and chargers are in the system. This provides a quick health check and encourages data entry.

**Why this priority**: Provides immediate feedback but is not critical to CRUD functionality. Useful for progress tracking and demo purposes.

**Independent Test**: Can be fully tested by creating partners/stations/chargers and verifying the stat cards on the Overview screen reflect real counts from the API.

**Acceptance Scenarios**:

1. **Given** the Overview screen, **When** the page loads, **Then** three StatCards appear showing total partners, total stations, and total chargers
2. **Given** the Overview screen with existing data, **When** the API is called, **Then** each StatCard displays the correct count from the database
3. **Given** the Overview screen, **When** a new partner is created in the Partners screen and the user returns to Overview, **Then** the partner count card updates automatically (or on next visit)

---

### Edge Cases

- What happens when the API is unreachable during form submission? System shows an error message and the form remains open with data preserved.
- What happens when a user tries to create a station with an empty address field? Validation prevents submission with an inline error message.
- What happens when a user edits a station and the API fails mid-request? An error toast/banner appears and the table reverts to the previous state.
- What happens when a user attempts to delete a partner that has associated stations? The delete request either succeeds with cascading deletes or returns a clear error message indicating the constraint.
- What happens when filter dropdowns load but the API has no data? Empty states or disabled dropdowns appear gracefully.

## Requirements

### Functional Requirements

- **FR-001**: System MUST allow users to create new partners with a name field via a modal form
- **FR-002**: System MUST display all partners in a sortable DataTable with name and created date columns
- **FR-003**: System MUST allow users to edit partner names via a modal and update the table without page reload
- **FR-004**: System MUST allow users to delete partners with a confirmation dialog
- **FR-005**: System MUST allow users to create stations with name, address, latitude, longitude, and partner selection
- **FR-006**: System MUST validate latitude range (-90 to 90) and longitude range (-180 to 180) before submission
- **FR-007**: System MUST display stations in a DataTable with name, address, partner name, and charger count columns
- **FR-008**: System MUST provide a partner filter dropdown on the Stations screen populated from the API
- **FR-009**: System MUST filter the stations table by the selected partner when the filter is applied
- **FR-010**: System MUST allow users to edit station details (name, address, coordinates) via a modal
- **FR-011**: System MUST allow users to delete stations with a confirmation dialog
- **FR-012**: System MUST allow users to create chargers with station selection, connector type, power kW, and status
- **FR-013**: System MUST display chargers in a DataTable with station name, connector type, power kW, and status badge columns
- **FR-014**: System MUST display status badges with standardized colors: available (green), in_use (amber), maintenance (red)
- **FR-015**: System MUST provide a station filter dropdown on the Chargers screen populated from the API
- **FR-016**: System MUST allow users to edit charger status via a modal (primary use case)
- **FR-017**: System MUST allow users to delete chargers with a confirmation dialog
- **FR-018**: System MUST display three StatCards on the Overview screen showing total count of partners, stations, and chargers
- **FR-019**: System MUST populate StatCard counts from the API in real time
- **FR-020**: System MUST show a loading skeleton while DataTable data is being fetched
- **FR-021**: System MUST show an empty state with a "Create [Entity]" prompt when a screen has no data
- **FR-022**: System MUST show an ErrorState with a retry button when the API is unreachable
- **FR-023**: System MUST display inline validation error messages on forms before submission
- **FR-024**: System MUST prevent form submission if required fields are missing or invalid
- **FR-025**: System MUST use design tokens from the shared token base for all colors, spacing, and typography

### Key Entities

- **Partner**: Represents an EV charging network operator. Attributes: id (UUID), name (string), created_at (timestamp). Relationships: Has many stations.
- **Station**: Represents a physical EV charging location. Attributes: id (UUID), partner_id (UUID), name (string), address (string), latitude (float), longitude (float), created_at (timestamp), updated_at (timestamp), charger_count (computed), available_count (computed). Relationships: Belongs to partner, has many chargers.
- **Charger**: Represents an individual charging unit at a station. Attributes: id (UUID), station_id (UUID), connector_type (string), power_kw (float), status (enum: available/in_use/maintenance), created_at (timestamp), updated_at (timestamp). Relationships: Belongs to station.

## Success Criteria

### Measurable Outcomes

- **SC-001**: A new partner created in the Dashboard appears in the Partners table within 1 second of form submission
- **SC-002**: A new station created for a partner appears in the Stations table with the correct partner name and charger count (0) within 1 second
- **SC-003**: A new charger created for a station appears in the Chargers table with the correct status badge color within 1 second
- **SC-004**: Editing any entity (partner name, station address, charger status) updates the table without requiring a page reload
- **SC-005**: Deleting an entity removes it from the table within 1 second
- **SC-006**: All filter dropdowns are populated with real API data (partners, stations) and reflect current state
- **SC-007**: Overview StatCards display the correct counts matching the database state
- **SC-008**: Form validation prevents submission of invalid data (missing fields, out-of-range coordinates) with visible error messages
- **SC-009**: API unreachable error is handled gracefully on all four screens with a retry option
- **SC-010**: Dashboard functions correctly on Chrome, Firefox, and Safari without visual regressions
- **SC-011**: Users can complete a full loop (create partner → station → chargers) in under 5 minutes
- **SC-012**: Empty states appear gracefully when no data exists (e.g., first time, after delete)

## Assumptions

- **Target Users**: Partner managers and admin users who manage charging infrastructure. No customer-facing exposure at this stage.
- **Authentication**: Authentication is out of scope for MVP-1. Dashboard is accessible without login.
- **Scope**: This feature is web-based only. Mobile dashboard is out of scope for MVP-1.
- **API Contract**: The backend API provides all required endpoints (partners, stations, chargers CRUD) with the response format documented in `docs/api/bornemap-service.md`.
- **Design Tokens**: A shared design token base exists at `source/packages/ui/` with `colors.ts` and `tailwind.config.base.js` that the Dashboard extends.
- **Form Validation**: Client-side validation happens before API submission. Server-side validation is handled by the backend API.
- **Data Persistence**: All changes are immediately persisted to the database via API. No local caching or conflict resolution is required.
- **Real-time Updates**: The Dashboard does not reflect changes made by other users. Page refresh is required to see updates from external sources.
- **Error Handling**: Network errors, validation errors, and 404/500 errors are displayed as user-friendly messages without technical details.
- **Empty State**: When no partners exist, the dashboard shows an empty state instead of failing. Users can still create the first partner.
- **Cascading Deletes**: Deleting a partner either cascades to stations and chargers or returns a clear error message. The specific behavior is determined by the backend and documented in the API.
