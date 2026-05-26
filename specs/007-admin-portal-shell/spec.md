# Feature Specification: Admin Portal — Shell, Navigation & BaseMap

**Feature Branch**: `007-admin-portal-shell`

**Created**: 2026-05-26

**Status**: Draft

**Input**: User description: "Phase 3 from plan_mvp0.md: Admin Portal — Shell, Navigation & BaseMap. Deliver a running admin portal with AppShell layout, sidebar navigation, interactive map showing station markers, design system package, and overview dashboard."

## Clarifications

### Session 2026-05-26

- Q: Marker click behavior — should clicking a station marker navigate directly to the detail page or show a popup first? → A: Show a popup with station info (name, city, available charger count) and a "View Details" link to navigate to the detail page.
- Q: Loading states on Overview Dashboard — should loading indicators be shown while data fetches? → A: Skeleton placeholders matching the shape of metric chips and map area, shown while data loads.
- Q: Sandbox toggle persistence — should the sandbox preference survive page reloads? → A: Persist in localStorage so the preference survives page reloads and browser restarts.

## User Scenarios & Testing

### User Story 1 — Admin navigates the portal via sidebar (Priority: P1)

An administrator logs into the BorneMap admin portal and is greeted by a persistent layout with a sidebar on the left and a main content area on the right. The sidebar contains six clearly labeled navigation items: Overview, Users, Data, Analytics, Security, and Settings. Clicking any item updates the main content area to show the corresponding section. The active section is visually highlighted in the sidebar.

**Why this priority**: P1 because the AppShell and navigation are the foundation upon which every other admin feature depends. Without a working layout and navigation, no other admin functionality is accessible.

**Independent Test**: Can be fully tested by opening the admin portal and clicking each of the six navigation items. Each click updates the main content area to a unique view. This delivers a navigable portal even if individual section content is placeholder.

**Acceptance Scenarios**:

1. **Given** an authenticated admin user, **When** the admin portal loads, **Then** the AppShell layout is visible with a sidebar on the left and a main content area on the right.
2. **Given** the AppShell layout, **When** the user clicks "Overview" in the sidebar, **Then** the Overview section loads in the main content area and "Overview" is highlighted in the sidebar.
3. **Given** the AppShell layout, **When** the user clicks each of the six navigation items, **Then** the main content area updates to a different section each time.
4. **Given** the AppShell layout, **When** the user navigates to a section, **Then** the URL updates to reflect the current section (e.g., `/overview`, `/users`, `/data`).

---

### User Story 2 — Admin views the Overview Dashboard with map and metrics (Priority: P1)

After logging in, the admin lands on the Overview Dashboard. The page displays three metric chips showing total counts for stations, chargers, and partners across the platform. Below the chips, an interactive map fills the remaining viewport and displays all station locations as green circular markers. The map is centered on Tunisia with an appropriate zoom level showing the entire country. Clicking a station marker navigates the admin to that station's detail view or opens a popup with station information.

**Why this priority**: P1 because the Overview Dashboard is the default landing page and the primary tool for administrators to understand platform health at a glance. The map provides the core spatial value proposition.

**Independent Test**: Can be fully tested by loading the admin portal — the landing page is the Overview Dashboard. Metric chips show numbers and the map displays station markers. This delivers immediate value to admins checking platform status.

**Acceptance Scenarios**:

1. **Given** the admin is on the Overview Dashboard, **When** the page loads, **Then** three metric chips display (total stations, total chargers, total partners) with non-negative integer values.
2. **Given** the Overview Dashboard, **When** the page loads, **Then** an interactive map is visible centered on Tunisia showing all station locations as green circular markers.
3. **Given** a station marker on the map, **When** the admin clicks it, **Then** a popup appears showing station name, city, and available charger count, with a "View Details" link to navigate to the full station detail page.

---

### User Story 3 — Admin uses the Sandbox Workspace toggle (Priority: P2)

An administrator wants to view and manage test data (is_test = true records) for verification purposes. The admin activates the Sandbox Workspace Selector toggle located in the portal header. When active, a prominent blue border appears at the top of the entire layout as a visual indicator. When sandbox mode is deactivated, the blue border disappears and test data is hidden from all views.

**Why this priority**: P2 because sandbox isolation is a constitutional requirement for data integrity, but the Overview Dashboard and navigation are prerequisites for the admin to reach the toggle.

**Independent Test**: Can be tested by toggling the sandbox selector on and off and observing the blue border indicator appear and disappear. Test data visibility verification depends on Phase 4 data views integration.

**Acceptance Scenarios**:

1. **Given** the admin is on any page in the portal, **When** the admin activates the Sandbox Workspace toggle, **Then** a prominent blue border (border-t-4 border-sky-500) appears at the top of the layout.
2. **Given** sandbox mode is active with the blue border visible, **When** the admin deactivates the toggle, **Then** the blue border disappears.

---

### User Story 4 — Admin sees consistent UI components and design tokens (Priority: P2)

As an admin navigates through different portal sections, all UI components follow a consistent visual language. Cards, dropdowns, tables, and modals share uniform styling — defined colors, border radii, spacing, and shadows. Data tables never wrap or break long content; instead they scroll horizontally when content exceeds viewport width. Destructive actions (delete) require the admin to type the exact resource identifier before the confirm button becomes enabled.

**Why this priority**: P2 because consistent design improves usability and trust, but the portal is functional (navigation + map) without it. The defensive UX patterns are critical for data safety but are exercised during CRUD operations in Phase 4.

**Independent Test**: Can be tested by visually inspecting a Settings card, a dropdown, and the scrollable table component. The consistent styling is visible immediately upon rendering any section.

**Acceptance Scenarios**:

1. **Given** any Settings card on the page, **When** it renders, **Then** it has rounded-2xl corners and proper shadow and padding consistent with the design system.
2. **Given** a data table with many columns, **When** the viewport is narrower than the table content, **Then** a horizontal scrollbar appears and content does not wrap or break.
3. **Given** a delete action is triggered, **When** the confirmation modal appears, **Then** the confirm button is disabled until the admin types the exact resource identifier into the input field.

---

### Edge Cases

- What happens when the map fails to load tiles? — The map area should show a fallback or empty state rather than breaking the page layout.
- What happens when the API is unreachable on Overview load? — Skeleton placeholders for metric chips and map should appear initially; if the API fails to respond, a brief error message replaces the skeleton and the map shows an empty state. The page must not crash or block other sections.
- What happens when there are zero stations/chargers/partners? — Metric chips should display 0, not fail or show nothing.
- What happens when sandbox mode is toggled rapidly? — The UI should not glitch or double-render the blue border.
- What happens on very small viewports (e.g., tablet)? — The sidebar should collapse or overlay rather than squeeze the content area.
- What happens when a user navigates directly to a deep URL (e.g., `/data/partners`) without clicking through the sidebar? — The correct section should load and the sidebar should highlight the active item.

## Requirements

### Functional Requirements

- **FR-001**: The admin portal MUST provide a persistent layout with a sidebar navigation panel and a main content area that updates when navigation items are clicked.
- **FR-002**: The sidebar MUST contain six navigation items: Overview, Users, Data, Analytics, Security, and Settings. Each item MUST have a unique icon and route.
- **FR-003**: The URL MUST update to reflect the active section (e.g., `/overview`, `/users`, `/settings/infrastructure-types`) and direct URL access MUST load the correct section.
- **FR-004**: The portal MUST include a Sandbox Workspace Selector toggle in the header. When activated, a prominent blue border indicator MUST appear at the top of the layout. When deactivated, the border MUST disappear. The toggle state MUST persist across page reloads via localStorage.
- **FR-005**: A design token system MUST be defined with centrally configured color, border radius, spacing, and shadow values. All UI components MUST use these tokens — no hardcoded style values.
- **FR-006**: A data table component MUST enforce a minimum content width of 800px and provide horizontal scrolling when the viewport is narrower than the content. Content MUST NOT wrap or break within cells.
- **FR-007**: A settings card component MUST have rounded corners, a card shadow, and consistent internal padding defined by the design token system.
- **FR-008**: A confirmation modal for destructive actions MUST require the user to type the exact full resource identifier (e.g., `STN-4f7d2a8b9c02`) before the confirm button becomes enabled.
- **FR-009**: The Overview Dashboard (landing page) MUST display three metric chips showing total stations, total chargers, and total partners. Non-numeric or zero values MUST display as 0.
- **FR-010**: The Overview Dashboard MUST display an interactive map. The map MUST be centered on Tunisia (latitude ~33.9, longitude ~9.5) at a zoom level that shows the entire country. Station locations MUST be shown as markers.
- **FR-011**: Clicking a station marker on the map MUST display a popup showing the station name, city, and available charger count. The popup MUST include a "View Details" link that navigates to the full station detail page.
- **FR-012**: The map MUST gracefully handle cases where API data is unavailable or tile loading fails — it MUST NOT crash the application or block other sections from loading.
- **FR-013**: While data is loading on the Overview Dashboard, skeleton placeholders MUST be shown matching the shape of the metric chips and the map area. Once data loads, the skeletons are replaced with actual content.
- **FR-014**: Placeholder cards MUST be shown on the Overview Dashboard for analytics features that are planned but not yet implemented (post-MVP0).

### Key Entities

- **AppShell**: The persistent layout container. Contains the sidebar navigation, header with sandbox toggle, and a main content area for section views. Acts as the structural foundation for all admin portal pages.
- **SidebarNav**: The navigation panel listing six portal sections. Highlights the active section and updates the main content area on click. Supports URL-based navigation for direct access to sections.
- **Overview Dashboard**: The default landing page. Shows aggregate metrics (station/charger/partner counts), an interactive map with station locations, skeleton placeholders while data loads, and placeholder cards for future analytics features.
- **BaseMap**: The interactive map component displaying station locations across Tunisia. Uses tile-based rendering with green circular markers for stations. Marker clicks show a popup with station info and a "View Details" link to the full detail page.
- **Sandbox Workspace Selector**: A toggle control in the header that switches between production data view and test data view. When active, a blue border indicator is shown across the layout top.
- **Design System**: A centralized token configuration governing colors, border radii, spacing, and shadows. All UI components reference these tokens. Includes specialized components: ScrollableTable for wide data tables, SettingsCard for configuration panels, SelectSetting for dropdown menus, and ConfirmDeleteModal for destructive action confirmation.

## Success Criteria

### Measurable Outcomes

- **SC-001**: Admin can navigate between all six portal sections in under 30 seconds on first use without assistance.
- **SC-002**: Overview Dashboard loads and displays metric chips with correct counts and an interactive map with station markers within 3 seconds on a standard internet connection.
- **SC-003**: Sandbox Workspace toggle activates and deactivates with the blue border indicator appearing/disappearing within 1 second of clicking.
- **SC-004**: All UI components in a given section use the same visual style (colors, radii, spacing) — no component has hardcoded style values that deviate from the design token system.
- **SC-005**: Data tables with 10+ columns remain readable without content wrapping when viewed on a 1366px-wide screen — horizontal scrolling is used instead.
- **SC-006**: The confirmation modal for destructive actions correctly requires full resource ID match — testing 10 attempts with wrong/partial IDs and 1 attempt with the correct ID must block 10 and allow 1.

## Assumptions

- Admin users are authenticated via the Phase 1 auth scaffold (JWT-based login) before accessing the portal. Auth UI is out of scope for this phase.
- The admin portal is a web application accessed via desktop/laptop browsers with minimum 1024px viewport width. Tablet support is graceful but not primary.
- The existing backend API (`/api/v1/stations`, `/api/v1/partners`) is available and returns data for the Overview Dashboard metrics and map markers.
- Station markers on the map show all stations (no sandbox filtering at this phase; sandbox filtering is a Phase 4 concern when data views are built).
- The map tiles are served from a public tile server (CartoDB light tiles) and require internet connectivity to load.
- Design tokens are defined in a Tailwind CSS configuration shared across all frontend packages — this matches the existing project convention.
- The sandbox toggle preference is persisted in localStorage and does not require backend API changes for this phase.
- React Router convention (nested routes with `<Outlet/>` pattern) is used for section routing, consistent with existing project patterns.
