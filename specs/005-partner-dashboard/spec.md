# Feature Specification: Partner Dashboard — Multi-Tenant Views

**Feature Branch**: `009-partner-dashboard`

**Created**: 2026-05-26

**Status**: Draft

**Input**: Phase 5 from docs/plan_mvp0.md

## User Scenarios & Testing

### User Story 1 — Partner views only their own stations and chargers (Priority: P1)

A partner operator logs into the partner dashboard and sees a filtered view containing only the stations and chargers they own. The dashboard provides the same CRUD capabilities as the admin portal but scoped entirely to the authenticated partner's data.

**Why this priority**: This is the core value proposition of the partner dashboard — multi-tenant isolation. Without it, partners cannot safely manage their infrastructure.

**Independent Test**: Log in as two different partner users. Each sees completely different sets of stations and chargers. Station A created by Partner 1 is invisible to Partner 2, and vice versa.

**Acceptance Scenarios**:

1. **Given** a partner user is authenticated, **When** they view the stations list, **Then** only stations where `owner_id` matches their partner profile are displayed
2. **Given** a partner user views the chargers list, **When** the list loads, **Then** only chargers belonging to their stations are shown
3. **Given** a partner user attempts to access a known station ID belonging to another partner (e.g., via direct URL entry), **When** they navigate to that station, **Then** the system returns a 403 or 404 response
4. **Given** a partner user views their dashboard, **When** the overview page loads, **Then** the station count, charger count, and any summary metrics reflect only their own data

---

### User Story 2 — Partner manages their stations with locked ownership (Priority: P1)

A partner operator can create, edit, and delete stations. When creating a station, the `owner_id` is automatically set to the authenticated partner's profile and cannot be changed. The partner sees only their own stations in the table. The stations page includes the BaseMap component (reused from admin portal) for bidirectional table↔map interaction — clicking a station row pans the map to its location, and clicking a marker on the map highlights the corresponding table row.

**Why this priority**: Partners must be able to add and update their infrastructure independently. Locking the owner prevents accidental or malicious ownership changes. The map provides spatial context for station placement.

**Independent Test**: A partner creates a new station with coordinates and details. The station appears in their list and on the map. Another partner cannot see it. The owner field is not editable. Clicking a table row pans the map; clicking a marker highlights the row.

**Acceptance Scenarios**:

1. **Given** a partner is on the stations page, **When** they click "Create Station", **Then** a modal form opens without an owner dropdown (owner is auto-assigned)
2. **Given** a partner submits a new station with valid data, **When** the creation succeeds, **Then** the station appears in their list with their partner profile as the owner and a marker appears on the map
3. **Given** a partner edits an existing station, **When** the edit modal opens, **Then** the owner field is either hidden or displayed as read-only
4. **Given** a partner attempts to delete a station, **When** they click delete, **Then** a confirmation modal appears requiring exact `STN-` ID match before enabling the confirm button
5. **Given** a partner views the stations page, **When** they click a station row, **Then** the map pans to that station's coordinates
6. **Given** a partner views the stations page, **When** they click a station marker on the map, **Then** the corresponding table row is highlighted

---

### User Story 3 — Partner manages chargers for their stations (Priority: P1)

A partner operator can create, edit, and delete chargers for their own stations. The charger station dropdown is pre-filtered to only show the partner's stations. Charger CRUD follows the same patterns as the admin portal.

**Why this priority**: Chargers are the operational unit that partners need to manage daily. Scoping to their stations is essential.

**Independent Test**: A partner creates a charger under one of their stations. The charger appears in the station's charger list. A different partner cannot see or modify this charger.

**Acceptance Scenarios**:

1. **Given** a partner is on the chargers page, **When** they open the create charger modal, **Then** the station dropdown lists only their own stations
2. **Given** a partner creates a charger, **When** the creation succeeds, **Then** the charger appears under the selected station's charger list
3. **Given** a partner edits a charger, **When** they change status (e.g., from available to occupied), **Then** the change is reflected immediately in the list
4. **Given** a partner deletes a charger, **When** they confirm with exact `CHG-` ID, **Then** the charger is hard-deleted and disappears from the list
5. **Given** a partner views a nested station detail page, **When** they see the charger list, **Then** only chargers belonging to that station are shown

---

### User Story 4 — Partner views and edits their own profile (Priority: P2)

A partner operator can see their partner profile details and update certain fields (display name, contact phone, logo URL). Classification and tax ID are read-only and cannot be changed without admin intervention.

**Why this priority**: Partners need basic profile management, but classification and tax ID changes require administrative oversight.

**Independent Test**: A partner navigates to their profile page, sees all fields populated, edits their display name successfully, and finds that tax ID and classification fields are disabled.

**Acceptance Scenarios**:

1. **Given** a partner navigates to their profile page, **When** the page loads, **Then** display name, contact phone, logo URL, classification, and tax ID are all displayed
2. **Given** a partner views the profile, **When** they attempt to edit, **Then** classification and tax ID fields are read-only
3. **Given** a partner updates their display name, **When** they save, **Then** the change is persisted and visible after page reload
4. **Given** a partner updates their contact phone, **When** they save, **Then** the change is persisted

---

### User Story 5 — Partner sees an Overview dashboard on login (Priority: P2)

The partner dashboard presents a simplified layout compared to the admin portal. The landing page after login is an Overview dashboard displaying metric chips (total stations, total chargers) and recent activity. This mirrors the admin portal pattern but scoped to the partner's own data.

**Why this priority**: An overview gives partners immediate insight into their infrastructure health without navigating to individual sections.

**Independent Test**: A partner logs in and sees an overview page with their station/charger counts. Metrics reflect only their own data.

**Acceptance Scenarios**:

1. **Given** a partner logs into the dashboard, **When** the app loads, **Then** the landing page at `/` is an Overview dashboard
2. **Given** a partner views the Overview dashboard, **When** the page loads, **Then** metric chips show their total stations and total chargers
3. **Given** a partner has zero stations, **When** the Overview dashboard loads, **Then** metric chips show 0 and an empty state is displayed

---

### User Story 6 — Partner dashboard UI scales down from admin portal (Priority: P2)

The sidebar contains four navigation items: Overview, Stations, Chargers, Profile. No Settings, Users, Analytics, or Security sections are shown. Design tokens and reusable components are shared from `@bornemap/ui`.

**Why this priority**: Partners should not be distracted or confused by administrative features that are irrelevant to them.

**Independent Test**: A partner logs in and sees exactly four navigation items. No admin-only pages are accessible or linked.

**Acceptance Scenarios**:

1. **Given** a partner logs into the dashboard, **When** the app shell renders, **Then** the sidebar contains exactly four items: Overview, Stations, Chargers, Profile
2. **Given** a partner navigates directly to an admin-only URL (e.g., `/settings` or `/users`), **When** the route is accessed, **Then** the system returns a 403 or redirects to the dashboard home

### Edge Cases

- What happens when a partner has zero stations? The stations and chargers pages show empty states with a prompt to create the first station.
- What happens when a partner's session expires? All API calls return 401 and the partner is redirected to the login page.
- What happens when a partner tries to create a charger without any stations? The station dropdown is empty; the create button is disabled with a tooltip explaining they need at least one station first.
- What happens when the backend is unreachable? All pages show error states with retry options, without crashing.
- What happens when a partner deletes all their stations? The chargers page becomes empty at the next refresh (cascading effect).
- What happens if an admin reassigns a station to a different partner? The station immediately disappears from the original partner's view on next fetch.
- What happens when multiple users from the same partner org edit the same station concurrently? Last-write-wins behavior applies (no optimistic locking in MVP0).

## Requirements

### Functional Requirements

- **FR-001**: Partner dashboard MUST authenticate users via the existing JWT-based auth system and validate the user has `role = partner`
- **FR-002**: All station and charger API endpoints MUST filter by the authenticated partner's `owner_id` at the repository layer
- **FR-003**: Station creation MUST automatically assign `owner_id` from the authenticated partner profile
- **FR-004**: Station edit forms MUST hide or disable the owner field (cannot be changed by partner)
- **FR-005**: Charger creation station dropdown MUST only list stations owned by the authenticated partner
- **FR-006**: Charger CRUD MUST be scoped to the partner's stations — creating a charger under another partner's station MUST return 403
- **FR-007**: Partner profile page MUST display display_name, contact_phone, logo_url, classification, and tax_id
- **FR-008**: Partner profile edit MUST allow changes to display_name, contact_phone, and logo_url only
- **FR-009**: Classification and tax_id fields in partner profile MUST be read-only
- **FR-010**: The landing page at `/` MUST display an Overview dashboard with metric chips showing the partner's total stations and total chargers
- **FR-011**: Partner dashboard sidebar MUST contain exactly four navigation items: Overview, Stations, Chargers, Profile
- **FR-012**: Direct navigation to admin-only routes (`/users`, `/settings`, `/analytics`, `/security`) MUST return 403 or redirect
- **FR-013**: All data views MUST show loading skeletons during API fetches
- **FR-014**: All data views MUST show error states with retry option when API calls fail
- **FR-015**: All destructive actions (delete station, delete charger) MUST use a confirmation modal requiring exact semantic ID match
- **FR-016**: Empty states MUST be shown when a partner has zero stations or zero chargers
- **FR-017**: Partner dashboard MUST reuse design tokens and components from `@bornemap/ui` (ScrollableTable, SettingsCard, ConfirmDeleteModal, SelectSetting, MetricChip)
- **FR-018**: Session expiration MUST trigger redirect to login page on any 401 response
- **FR-019**: Multiple user accounts MAY be associated with the same partner profile, with all users sharing access to the same partner's stations and chargers

### Key Entities

- **Partner Profile**: Represents the business or private operator. Key attributes: display_name, classification (Business/Private), tax_id, contact_phone, logo_url. May have multiple associated User accounts.
- **Station**: A physical charging location owned by a partner. Key attributes: name, address, city, coordinates (lng/lat), is_operational, is_test. Scoped by `owner_id` to enforce multi-tenancy. Deletion is soft (sets `deleted_at`), consistent with admin portal.
- **Charger**: An individual charging unit at a station. Key attributes: connector_type, power_kw, current_type (AC/DC), status (available/occupied/faulted/offline). Belongs to a station, transitively scoped to the station's partner owner.
- **User**: An authenticated account. Key attributes: email, role (partner/admin/driver). Partner users have a corresponding partner profile. Multiple users can be associated with the same partner org.

## Success Criteria

### Measurable Outcomes

- **SC-001**: A partner operator can log in and see their station list with all data loading within 3 seconds
- **SC-002**: Two different partner operators viewing the same page at the same time see completely disjoint data sets (zero overlap)
- **SC-003**: A partner can create a station with coordinates and have it appear on their map and table within 2 seconds
- **SC-004**: Accessing another partner's station ID via direct URL returns a 403/404 error in every case (100% isolation guarantee)
- **SC-005**: A partner with zero infrastructure sees a helpful empty state (not a blank screen or error)
- **SC-006**: All data views gracefully handle backend unavailability by showing error states without crashing the application
- **SC-007**: Partners complete station creation in under 5 steps and under 2 minutes (including form fill)

## Clarifications

### Session 2026-05-26

- Q: What is the landing page after a partner logs in? → A: Overview dashboard with metric chips (total stations, total chargers), mirroring the admin portal pattern
- Q: Is partner station deletion soft-delete or hard-delete? → A: Soft-delete (consistent with admin portal behavior, recoverable by admin)
- Q: Should the partner Stations page include the BaseMap for bidirectional table↔map interaction? → A: Yes, include the BaseMap (reused from admin portal)
- Q: Can a partner organization have multiple user accounts? → A: Yes, multiple users per partner org. Each user has their own login and all see the same partner data.

## Assumptions

- Existing JWT authentication middleware from Phase 1 is reused — no changes to auth infrastructure
- Backend already supports `owner_id` injection from JWT claims for partner-scoped endpoints
- Existing CRUD endpoints for stations and chargers already exist (Phase 1 completed) and only require scoping adjustments
- The admin portal and partner dashboard share the same backend — no separate backend deployment
- Reusable UI components from `@bornemap/ui` are already built (Phase 3) and available
- Partners are created by admins in the admin portal (not self-registered)
- Logo_url is stored as a URL string (image hosting is out of scope for this spec)
- Mobile driver app is already implemented (Phase 6) and does not need partner dashboard functionality
- The base UI package `@bornemap/ui` is already installed and importable in the partner-dashboard app
