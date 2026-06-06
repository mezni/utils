# Feature Specification: Driver Web App with Mock Data

**Sprint Branch**: `002-driver-web-mock`

**Created**: 2026-06-05

**Status**: Draft

**Input**: Driver Web App is fully navigable with all public screens populated from realistic mock data (Sprint 1.2 of Phase 1)

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Browse Stations on Map (Priority: P1)

A driver visiting the BorneMap website can immediately see charging stations on a map, browse nearby stations, and get an overview of charger availability — without any account or login.

**Why this priority**: Public access to station discovery is the core value proposition (Principle VI — Public Access First). This is the primary use case for every visitor and must work without authentication.

**Independent Test**: Can be fully tested by loading the home page URL. The map area renders with station markers, and the sidebar shows a list of station cards with charger counts. No login required.

**Acceptance Scenarios**:

1. **Given** a driver visits the root URL `/`, **When** the page loads, **Then** a full-bleed map placeholder is visible with mock station markers positioned on it
2. **Given** the map is displayed, **When** the driver views the sidebar, **Then** a scrollable list of StationCard components shows station name, address, distance, charger count, and availability badge
3. **Given** the driver sees the search bar, **When** they view the top of the screen, **Then** a SearchBar and FilterPills row are visible above the station list

---

### User Story 2 - View Station Details & Reviews (Priority: P1)

A driver can tap a station card or marker to see full station details, including available chargers with connector types and power ratings, plus user reviews with star ratings.

**Why this priority**: After discovering a station, the driver needs detailed information to decide whether to visit. This completes the core discovery journey.

**Independent Test**: Can be fully tested by clicking a station card on the home page and navigating to the station detail screen.

**Acceptance Scenarios**:

1. **Given** a driver clicks a StationCard, **When** the Station Detail screen loads, **Then** station info (name, address, map) is displayed with a list of ChargerRow components showing connector type, power kW, and availability status
2. **Given** the driver scrolls to the reviews section, **When** they view ReviewCard components, **Then** each review shows star rating, date, reviewer name, and text content (Arabic or French)
3. **Given** the driver sees the charger list, **When** a charger is unavailable, **Then** its status badge shows "unavailable" with appropriate styling

---

### User Story 3 - Search, Filter & Favorites (Priority: P2)

A driver can search for stations by text, filter by charger type, and save favorite stations for quick access later.

**Why this priority**: Search and filtering enhance the core discovery experience. Favorites requires mock data only — no backend auth yet.

**Independent Test**: Can be tested by navigating to Search Results and Favorites screens independently. Search Results show filtered station cards. Favorites show saved stations or empty state.

**Acceptance Scenarios**:

1. **Given** a driver types in the SearchBar and presses enter, **When** the Search Results screen loads, **Then** matching StationCard results are displayed with SearchBar and FilterPills at the top
2. **Given** search returns no results, **When** the screen renders, **Then** an EmptyState component is shown with appropriate messaging
3. **Given** the driver navigates to Favorites, **When** the screen loads, **Then** saved StationCard items are displayed, or an EmptyState if no favorites exist

---

### User Story 4 - Profile & Login/Register (Priority: P3)

A driver can view a profile page with static form fields and see login/register screens with mock social login buttons.

**Why this priority**: These screens complete the application's navigation coverage. They are static (no submission) and provide the layout for when auth is implemented in Phase 4.

**Independent Test**: Can be tested by navigating to Profile and Login/Register screens. Profile shows form layout. Login/Register shows centered card with input fields and social login buttons.

**Acceptance Scenarios**:

1. **Given** a driver navigates to Profile, **When** the screen renders, **Then** form layout with Input fields (name, email, phone) and a Button are displayed — no submission logic
2. **Given** a driver navigates to Login/Register, **When** the screen renders, **Then** a centered card with Input fields and social login buttons (Google, Apple, Facebook icons) is displayed

---

### Edge Cases

- What happens when the screen is very narrow (< 640px)? Sidebar collapses or becomes full-width, FilterPills wrap correctly
- What happens when RTL is active (Arabic)? Entire layout flips — sidebar on right, map on left, text alignment reversed, icons reordered
- What happens when station has no chargers? StationCard shows "0 chargers" with appropriate badge
- What happens when station has no reviews? Reviews section displays EmptyState "No reviews yet"
- What happens on direct URL navigation (e.g., `/stations/STN-001`)? Station Detail screen renders correctly with matching data
- How does zoom control work? ZoomControls show +/- buttons, buttons are present but non-functional (map is placeholder)
- What happens when the user refreshes on a Favorites page? Favorites render from mock data (session-based, no persistence)

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: `apps/driver-web` MUST be scaffolded with Vite + React + TypeScript
- **FR-002**: Tailwind config MUST extend `packages/ui/tailwind.config.base.js` to consume design tokens
- **FR-003**: i18n MUST be configured with Arabic (`ar.json`) and French (`fr.json`) translations for all static strings
- **FR-004**: RTL layout MUST work correctly on every screen when Arabic is selected (dir="rtl" on `<html>`)
- **FR-005**: Router MUST be configured with routes for all 6 screens: `/` (Home/Map), `/stations/:id` (Station Detail), `/search` (Search Results), `/favorites` (Favorites), `/profile` (Profile), `/login` (Login/Register)
- **FR-006**: Mock data MUST include at least 15 stations with real Tunisian addresses and coordinates
- **FR-007**: Mock data MUST include 2–4 chargers per station with realistic connector types (Type 2, CCS, CHAdeMO) and power ratings (3.7–350 kW)
- **FR-008**: Mock data MUST include 3–5 reviews per station with Arabic and French content
- **FR-009**: Home/Map screen MUST render: full-bleed map placeholder (#EAF0E6), mock station markers as positioned divs, SearchBar, FilterPills, StationCard list in sidebar, ZoomControls
- **FR-010**: Station Detail screen MUST render: station info, ChargerRow list, ReviewCard list, rating summary
- **FR-011**: Search Results screen MUST render: SearchBar, FilterPills, paginated StationCard list, EmptyState when no results
- **FR-012**: Favorites screen MUST render: StationCard list or EmptyState
- **FR-013**: Profile screen MUST render: form layout with Input fields (name, email, phone) and Button — static, no submission
- **FR-014**: Login/Register screen MUST render: centered card, Input fields (email, password), social login buttons with icons — static
- **FR-015**: Navigation between all 6 screens MUST work (click, back button, direct URL)
- **FR-016**: No backend calls MUST be made — all data comes from mock data files
- **FR-017**: Each driver-specific component (MobileTopBar, SearchBar, FilterPills, MapPinMarker, ZoomControls, StationCard, ChargerRow, ReviewCard, BottomStationCard) MUST be in `apps/driver-web/src/components/`
- **FR-018**: MobileTopBar MUST show menu icon, brand name, and notification bell
- **FR-019**: SearchBar MUST show search icon, text input, and float in a card-style container
- **FR-020**: FilterPills MUST render as horizontal pill row with active/inactive visual states
- **FR-021**: MapPinMarker MUST support default, selected, and unavailable states with appropriate colors and glow shadow
- **FR-022**: ZoomControls MUST render as +/- button group
- **FR-023**: StationCard MUST show station name, address, distance, charger count, and availability badge
- **FR-024**: ChargerRow MUST show connector type, power in kW, and status badge
- **FR-025**: ReviewCard MUST show star rating, date, reviewer name, and review text
- **FR-026**: BottomStationCard MUST show station summary with specification rows

### Key Entities

- **Station**: Charging station entity with name, address (Tunisian), coordinates (lat/lng), distance, charger count, availability status
- **Charger**: Individual charging point at a station with connector type (Type 2, CCS, CHAdeMO), power (kW), availability status
- **Review**: User review of a station with star rating (1–5), date, author name, text content (Arabic or French)
- **Driver User**: Mock user profile with name, email, phone, favorite station IDs, avatar

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: All 6 screens render with realistic mock data when navigated to
- **SC-002**: Navigation between all screens works via click, browser back button, and direct URL entry
- **SC-003**: Arabic RTL layout is visually correct on every screen (sidebar on right, flipped layout, no broken elements)
- **SC-004**: French layout renders correctly with translated strings on every screen
- **SC-005**: No backend calls are made — verified by checking network tab or console for API requests
- **SC-006**: All 9 driver-specific components render correctly with required props and states
- **SC-007**: `pnpm build` completes successfully for `apps/driver-web` with zero warnings
- **SC-008**: All static strings are translated in both `ar.json` and `fr.json`

## Accessibility Requirements *(mandatory)*

- **WCAG 2.1 AA**: The Driver Web App MUST meet WCAG 2.1 AA accessibility compliance on all screens
- **Keyboard Navigation**: All interactive elements (buttons, links, inputs) MUST be keyboard accessible (Tab, Enter/Space)
- **Focus Indicators**: Visible focus indicators on all interactive elements
- **Color Contrast**: All text MUST have ≥ 4.5:1 contrast ratio on background, ≥ 3:1 for large text
- **ARIA Labels**: Map markers, interactive controls, and status indicators MUST have appropriate ARIA labels
- **RTL Accessibility**: Screen reader behavior MUST be correct in Arabic mode (proper pronunciation, correct reading order)
- **Language Attributes**: `<html>` element MUST have correct `lang` and `dir` attributes based on selected language

## Assumptions

- Design tokens from `packages/ui` (Sprint 1.1) are available and importable
- Shared components from `packages/ui` (Button, Input, Badge, StatusBadge, Skeleton, EmptyState, ErrorState) are available and working
- The monorepo is already configured with pnpm workspaces
- Map is a placeholder (green #EAF0E6 background with positioned marker divs) — no real map library in this sprint
- Social login buttons are visual only with icons — no OAuth implementation
- Favorites are mock data only — no persistence
- Profile form is static — no submission or validation
- Mock data will be replaced with real API calls in Phase 5

**Dependency on existing system/service**:
- Requires `packages/ui` to be built and available in the workspace
- Requires pnpm workspace configuration to resolve local packages
- Requires design token and shared component packages from Sprint 1.1
