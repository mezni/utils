# Sprint Specification: Dashboard App with Mock Data

**Sprint Branch**: `004-dashboard-mock`

**Created**: 2026-06-06

**Status**: Draft

**Input**: User description: "read docs/core/implementation-plan.md sprint 1.4"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Partner Dashboard Navigation (Priority: P1)

A partner user logs into the dashboard application and sees an overview of their station network with key performance metrics. They can navigate between different sections (Overview, My Stations, Station Edit, Charger Management, Availability Update, Reports) to manage their charging infrastructure.

**Why this priority**: This is the core value proposition for partners - the ability to see and manage their station portfolio. Without this, partners cannot derive value from the platform.

**Independent Test**: Can be fully tested by logging in with a mock partner role, verifying the Overview displays correct metrics, and successfully navigating between all partner-specific screens. Delivers complete partner management UI experience.

**Acceptance Scenarios**:

1. **Given** a partner user is logged in, **When** they load the Overview screen, **Then** they see 4 stat cards (total stations, total chargers, total reviews, availability percentage) and a data card listing their stations
2. **Given** a partner user is on the My Stations screen, **When** they view the station table, **Then** they see station name, charger count, status, and action buttons (edit, manage chargers, update availability)
3. **Given** a partner user is on the Station Edit screen, **When** they view the form, **Then** they see input fields and a select dropdown for managing chargers (static form, no submission)
4. **Given** a partner user is on the Charger Management screen, **When** they view the table, **Then** they see connector type, power rating, status, and action buttons for each charger
5. **Given** a partner user is on the Availability Update screen, **When** they view the table, **Then** they see toggle controls for each charger status (static UI, no state changes)
6. **Given** a partner user is on the Reports screen, **When** they view the page, **Then** they see 4 stat cards with usage metrics and a data card with chart placeholder

---

### User Story 2 - Admin Dashboard Navigation (Priority: P1)

An admin user logs into the dashboard application and sees a comprehensive platform overview with metrics for all users, partners, stations, chargers, reviews, and events. They can navigate between different sections (Overview, Users, Partners, Stations, Chargers, Reviews, Reports) to manage the entire platform.

**Why this priority**: This provides the administrative control and oversight needed for platform operations. Without this, admins cannot manage users, partners, or moderate content.

**Independent Test**: Can be fully tested by logging in with a mock admin role, verifying the Overview displays comprehensive platform metrics, and successfully navigating between all admin-specific screens. Delivers complete admin management UI experience.

**Acceptance Scenarios**:

1. **Given** an admin user is logged in, **When** they load the Overview screen, **Then** they see 6 stat cards (total users, total partners, total stations, total chargers, total reviews, total events), a data card with live station list, and a data card with active drivers table
2. **Given** an admin user is on the Users screen, **When** they view the table, **Then** they see user name, email, role, status, and action buttons
3. **Given** an admin user is on the Partners screen, **When** they view the table, **Then** they see partner name, station count, and action buttons
4. **Given** an admin user is on the Stations screen, **When** they view the table, **Then** they see station name, partner, status, and action buttons
5. **Given** an admin user is on the Chargers screen, **When** they view the table, **Then** they see station name, connector type, power rating, and status
6. **Given** an admin user is on the Reviews screen, **When** they view the table, **Then** they see station name, user name, rating, review text, and moderation action buttons
7. **Given** an admin user is on the Reports screen, **When** they view the page, **Then** they see 6 stat cards with analytics metrics and data cards with chart placeholders

---

### User Story 3 - Language Switching and RTL Support (Priority: P2)

A partner or admin user can switch between Arabic (RTL) and French languages. When Arabic is selected, the entire dashboard interface switches to right-to-left layout with correct alignment for sidebar, tables, and forms.

**Why this priority**: Arabic is a primary language for the target market (Tunisia). RTL support is a constitutional requirement and a Class A bug if broken.

**Independent Test**: Can be fully tested by switching to Arabic language on any screen and verifying that sidebar aligns to the right, tables are properly formatted for RTL, and form elements are correctly aligned. Delivers Arabic language compliance.

**Acceptance Scenarios**:

1. **Given** the dashboard is loaded in French, **When** user switches to Arabic, **Then** the sidebar aligns to the right and navigation items display in Arabic
2. **Given** the dashboard is in Arabic, **When** user views a data table, **Then** the table columns display correctly with proper RTL alignment
3. **Given** the dashboard is in Arabic, **When** user views a form, **Then** form labels and input fields align correctly for RTL
4. **Given** the dashboard is in Arabic, **When** user switches back to French, **Then** the interface returns to left-to-right layout with all elements properly aligned

---

### User Story 4 - Development Role Switching (Priority: P2)

A developer can toggle between partner and admin roles via a dev-only UI control to test both interfaces without requiring authentication. This role switcher will be removed when real authentication is introduced in Phase 4.

**Why this priority**: Enables efficient development and testing of both role-based interfaces without implementing full authentication yet.

**Independent Test**: Can be fully tested by clicking the dev-only role toggle and verifying the navigation menu and screen content changes between partner and admin views. Delivers role switching capability for development.

**Acceptance Scenarios**:

1. **Given** the dashboard is loaded, **When** user toggles from partner to admin role, **Then** the navigation menu changes to admin-specific items and Overview displays admin metrics
2. **Given** the dashboard is in admin mode, **When** user toggles to partner role, **Then** the navigation menu changes to partner-specific items and Overview displays partner metrics
3. **Given** the dashboard is loaded, **When** user views the role toggle control, **Then** it displays as a dev-only element that is clearly distinguishable from production UI

---

### Edge Cases

- What happens when mock data arrays are empty for a specific screen? (Display empty state with appropriate message)
- How does system handle navigation to a screen that doesn't exist for the current role? (Hide or disable navigation items based on role)
- What happens when a user switches roles while on a screen that doesn't exist in the new role? (Redirect to the Overview screen of the new role)
- How does system handle Arabic language strings that are longer than French versions? (Layout should accommodate variable text lengths)

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Application MUST display partner overview with 4 stat cards (stations, chargers, reviews, availability) when logged in as partner
- **FR-002**: Application MUST display admin overview with 6 stat cards (users, partners, stations, chargers, reviews, events) when logged in as admin
- **FR-003**: Application MUST provide a left sidebar navigation with BrandHeader, NavLinks, and BottomActions
- **FR-004**: Application MUST display navigation items based on current user role (partner: Overview, My Stations, Station Edit, Charger Management, Availability Update, Reports; admin: Overview, Users, Partners, Stations, Chargers, Reviews, Reports)
- **FR-005**: Application MUST render NavigationItem with icon, label, optional badge, and active state indicator
- **FR-006**: Application MUST provide a top bar with tab navigation on the left and operator name and avatar on the right
- **FR-007**: Application MUST display sortable, paginated DataTable components with row actions
- **FR-008**: Application MUST render DataCard panels with CardHeader and body slot for content
- **FR-009**: Application MUST provide scrollable PageContent area with surface.background canvas
- **FR-010**: Application MUST support Arabic (RTL) and French languages with complete translation
- **FR-011**: Application MUST switch sidebar alignment (left for French, right for Arabic) based on selected language
- **FR-012**: Application MUST align tables correctly in RTL layout for Arabic language
- **FR-013**: Application MUST align form elements correctly in RTL layout for Arabic language
- **FR-014**: Application MUST provide dev-only role toggle control to switch between partner and admin modes
- **FR-015**: Application MUST hide navigation items that are not accessible for the current user role
- **FR-016**: Application MUST redirect to Overview screen when user switches to a role where the current screen doesn't exist
- **FR-017**: Application MUST display empty state messages when mock data arrays are empty
- **FR-018**: Application MUST populate all screens with mock data from local files (no backend calls)
- **FR-019**: Application MUST use design tokens from `packages/ui` for all visual values (colors, spacing, typography, shadows, radius)

### Key Entities

- **Partner**: Organization that owns and operates charging stations; has name, station count, status; related to stations and users
- **Station**: Charging location with coordinates, address, charger count; belongs to a partner; has many chargers; related to reviews
- **Charger**: Charging connector with type, power rating, status; belongs to a station
- **User**: Platform user with name, email, role (partner, admin, registered_driver), status
- **Review**: User feedback with rating, text, date; belongs to a user and a station
- **Report**: Statistical metric with label, value, trend; used for dashboard KPI cards

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Partner users can navigate to all 6 partner-specific screens (Overview, My Stations, Station Edit, Charger Management, Availability Update, Reports) within 2 clicks from the Overview screen
- **SC-002**: Admin users can navigate to all 7 admin-specific screens (Overview, Users, Partners, Stations, Chargers, Reviews, Reports) within 2 clicks from the Overview screen
- **SC-003**: Arabic RTL layout displays correctly on all screens with sidebar aligned to the right and all tables properly formatted
- **SC-004**: Role switching between partner and admin modes completes within 1 second and correctly updates navigation menu and screen content
- **SC-005**: All 15 mock stations display correctly in data tables with correct formatting and alignment in both Arabic and French
- **SC-006**: Application loads and displays the Overview screen within 3 seconds on initial page load
- **SC-007**: All design token values resolve correctly with zero hardcoded visual values in the codebase

## Assumptions

- Target users have modern web browsers (Chrome, Firefox, Safari) with JavaScript enabled
- RTL layout support will be tested using standard browser dev tools language switching
- Mock data files will reuse the same 15 stations, 50+ chargers, and 60+ reviews from driver apps to ensure consistency
- Dev-only role toggle will be implemented as a floating button or dropdown in the corner of the screen
- Chart placeholders will be simple gray rectangles or placeholder graphics, not actual chart libraries
- Navigation between screens will use client-side routing without page reloads
- Form inputs (Station Edit, Availability Update) will be static mock forms without validation or submission logic
- Role-based access control is mocked and will be replaced with real authentication in Phase 4