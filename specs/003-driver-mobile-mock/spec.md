# Sprint Specification: Driver Mobile App with Mock Data

**Sprint Branch**: `003-driver-mobile-mock`

**Created**: 2026-06-05

**Status**: Draft

**Input**: User description: "read docs/core/project/implementation-plan.md sprint 1.3"

## User Scenarios & Testing

### User Story 1 — Browse Stations on Map (Priority: P1)

As a driver, I want to open the app and see nearby charging stations on a full-bleed map with a bottom sheet showing the nearest station, so that I can quickly assess my charging options.

**Why this priority**: The map view is the primary entry point and first screen users see. Without it, the app provides no value.

**Independent Test**: Can be fully tested by opening the app and verifying the map background renders with mock pin markers and the bottom card shows the first station's details.

**Acceptance Scenarios**:

1. **Given** the app is opened, **When** the home screen loads, **Then** a full-bleed `#EAF0E6` background fills the screen
2. **Given** the map background is displayed, **When** mock data is loaded, **Then** at least 10 station pin markers are visible as positioned Views
3. **Given** pin markers are displayed, **When** the user taps a pin, **Then** the BottomStationCard updates to show that station's info
4. **Given** a station is selected on the map, **When** the BottomStationCard is visible, **Then** it shows station name, address, availability badge, distance, charger count, and rating
5. **Given** the home screen is open, **When** the user taps the SearchBar, **Then** navigation moves to the Search screen

---

### User Story 2 — Browse Station List (Priority: P1)

As a driver, I want to see a scrollable list of all stations with key details, so that I can browse options without using the map.

**Why this priority**: The list view complements the map and is essential for drivers who prefer tabular browsing. It is the second-most-accessed screen.

**Independent Test**: Can be tested by tapping the Station List tab and verifying all 15 mock stations appear in a FlatList with StationCard items.

**Acceptance Scenarios**:

1. **Given** the user is on any screen, **When** they tap the Station List tab, **Then** a FlatList displays all mock stations
2. **Given** the station list is displayed, **When** the user pulls down, **Then** a pull-to-refresh spinner appears (no-op on mock data)
3. **Given** the station list is loading for the first time, **Then** Skeleton components display as placeholders
4. **Given** any station card in the list, **When** the user taps it, **Then** navigation moves to the Station Detail screen

---

### User Story 3 — View Station Details (Priority: P2)

As a driver, I want to see full station details including all chargers and reviews, so that I can decide whether to go to this station.

**Why this priority**: This screen provides the detailed information drivers need to make decisions. It is a core feature but depends on the list and map screens for entry.

**Independent Test**: Can be tested by navigating from the map or station list to a detail screen and verifying charger rows and review cards render with mock data.

**Acceptance Scenarios**:

1. **Given** a station detail screen, **When** it loads, **Then** it shows station name, address, rating, distance, and charger count
2. **Given** the station detail screen, **When** it has chargers, **Then** each charger shows connector type, power (kW), price, and availability badge
3. **Given** the station detail screen, **When** it has reviews, **Then** each review shows author name, star rating, date, and text
4. **Given** the station has no chargers, **When** the screen loads, **Then** an empty state message is shown

---

### User Story 4 — Search and Filter Stations (Priority: P2)

As a driver, I want to search stations by name or address and filter by charger type and availability, so that I can quickly find a station that meets my needs.

**Why this priority**: Search and filter significantly improve usability but are not essential for the initial map browsing experience.

**Independent Test**: Can be tested by entering a search query in the Search TextInput and verifying the results list updates, then applying filter pills and verifying further refinement.

**Acceptance Scenarios**:

1. **Given** the search screen, **When** the user types in the TextInput, **Then** results update in real-time with debounce
2. **Given** search results are displayed, **When** the user selects a filter pill (Type2, CCS, CHAdeMO), **Then** results are filtered by connector type
3. **Given** search results are displayed, **When** the user selects the availability filter, **Then** only available stations are shown
4. **Given** the search query matches no stations, **Then** an EmptyState component is displayed
5. **Given** the search screen, **When** the user clears the search text, **Then** all stations are displayed again

---

### User Story 5 — Manage Favorites (Priority: P3)

As a registered driver, I want to save favorite stations and view them in a dedicated list, so that I can quickly access my preferred stations.

**Why this priority**: Favorites improve the experience for repeat users but are not needed for first-time browsing. Mock data only — no persistence.

**Independent Test**: Can be tested by favoriting stations from the Station Card and verifying they appear in the Favorites tab, and unfavoriting removes them.

**Acceptance Scenarios**:

1. **Given** any station card, **When** the user taps the heart icon, **Then** the icon fills in and the station is added to favorites
2. **Given** the favorites screen, **When** it opens, **Then** all favorited stations are shown in a FlatList
3. **Given** the favorites screen has no favorites, **Then** an EmptyState component is displayed
4. **Given** a favorite station card, **When** the user taps the heart icon again, **Then** the station is removed from favorites

---

### User Story 6 — Profile and Authentication (Priority: P3)

As a driver, I want to view my profile and log in or register, so that I can manage my account.

**Why this priority**: Profile and auth screens are needed for the app to be complete but are static in this sprint (no backend). They are mock-only placeholders.

**Independent Test**: Can be tested by navigating to the Profile tab and verifying the static form renders, then tapping Login to see the login/register screen with social login buttons.

**Acceptance Scenarios**:

1. **Given** the profile screen, **When** it loads, **Then** it displays avatar, name, email, and phone inputs with mock values
2. **Given** the profile screen, **When** the user taps "Login" (or similar), **Then** the login/register screen appears
3. **Given** the login/register screen, **When** it loads, **Then** it shows email and password fields, and social login buttons (Google, Apple, Facebook) — all visual-only
4. **Given** the login/register screen, **When** the user taps the register tab, **Then** the form switches to registration mode

---

### Edge Cases

- What happens when the device font size is set to maximum? All text must remain readable without truncation
- What happens when the user rotates the device? Layout should not break (lock to portrait is acceptable)
- What happens on a low-end Android device? Animations should not stutter; FlatList should handle 15 items efficiently
- What happens when i18n language is changed at runtime? The UI must immediately reflect the new language and RTL direction

## Requirements

### Functional Requirements

- **FR-001**: System MUST display a full-bleed `#EAF0E6` background on the Map/Home screen as a map placeholder
- **FR-002**: System MUST render at least 10 station pin markers as absolutely positioned Views on the map placeholder
- **FR-003**: System MUST display a BottomStationCard showing the selected or first station's details (name, address, availability, distance, charger count, rating)
- **FR-004**: System MUST provide a bottom tab navigator with tabs: Map, Stations List, Search, Favorites, Profile
- **FR-005**: System MUST display a FlatList of StationCard items on the Station List screen
- **FR-006**: System MUST support pull-to-refresh on the Station List screen (no-op on mock data)
- **FR-007**: System MUST show Skeleton placeholders on the first load of the Station List screen
- **FR-008**: System MUST display a Station Detail screen with station info, ChargerRow FlatList, and ReviewCard FlatList
- **FR-009**: System MUST display an EmptyState when a station has no chargers or no reviews
- **FR-010**: System MUST provide a Search screen with TextInput, debounced filtering, and FilterPills for charger type and availability
- **FR-011**: System MUST display an EmptyState on the Search screen when no results match the query
- **FR-012**: System MUST allow users to toggle favorite status on station cards with a heart icon
- **FR-013**: System MUST display a Favorites screen with a FlatList of favorited stations
- **FR-014**: System MUST display an EmptyState on the Favorites screen when no stations are favorited
- **FR-015**: System MUST display a Profile screen with static form fields (name, email, phone) pre-filled with mock user data
- **FR-016**: System MUST display a Login/Register screen with email and password fields and social login buttons (Google, Apple, Facebook) — all visual-only, no submission
- **FR-017**: System MUST display all static strings in Arabic or French based on the device language setting
- **FR-018**: System MUST switch layout direction to RTL when Arabic language is active
- **FR-019**: System MUST use native design tokens from `packages/ui/src/tokens/native.ts` for all visual values
- **FR-020**: System MUST NOT make any network calls to backend services — all data from local mock files
- **FR-021**: System MUST set safe area insets on MobileTopBar (top) and BottomTabBar (bottom) using platform APIs
- **FR-022**: System MUST display a CenterActionButton as a raised circular button above the BottomTabBar
- **FR-023**: System MUST display ZoomControls as a floating button group on the map screen
- **FR-024**: System MUST implement SpecRow component for displaying detail rows with label and value

### Key Entities

- **Station**: Charging station entity with id, name, address, coordinates, distance, charger/available counts, availability status, rating, review count. Same shape as web mock data.
- **Charger**: Individual charger unit with id, station relationship, connector type (Type2/CCS/CHAdeMO), power in kW, availability status, price per kWh, maintenance date.
- **Review**: User-submitted review with id, station relationship, author name, star rating, text content, date, language (ar/fr/en).
- **DriverUser**: Driver profile with id, name, email, phone, avatar, favorite station IDs, language preference.

## Success Criteria

### Measurable Outcomes

- **SC-001**: All 7 screens render with realistic mock data on both iOS simulator and Android emulator
- **SC-002**: Navigation between all screens works via bottom tabs and stack navigation (forward and back)
- **SC-003**: Arabic RTL layout is correct on every screen — verified on both iOS and Android
- **SC-004**: French layout renders correctly with translated strings on all screens
- **SC-005**: No backend calls are made — verified via network inspector
- **SC-006**: All driver-specific mobile components render with required props and all visual states
- **SC-007**: `pnpm build` passes for `apps/driver-mobile` with zero warnings

## Assumptions

- All mock data files will use the same shape as the web mock data in Sprint 1.2, allowing potential sharing
- Map is a placeholder `#EAF0E6` background View — no real map library is used in this sprint
- Navigation uses React Navigation (bottom tabs + stack) as the standard React Native navigation library
- Safe area handling uses `react-native-safe-area-context` for insets
- RTL switching uses React Native's built-in `I18nManager` with `forceRTL`
- Social login buttons are visual-only — no OAuth integration in this sprint
- Profile form is static — no submission or validation
- Favorites are mock-only with no persistence across app restarts
- i18n strings can reuse the same `ar.json` and `fr.json` translation files from the web app, adapted for mobile-specific keys
- FlatList performance is sufficient for 15 items — no virtualized list optimization needed
- The app targets both iOS and Android from a single codebase via Expo
