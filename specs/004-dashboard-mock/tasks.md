---

description: "Task list for Dashboard App with Mock Data sprint implementation"

---

# Tasks: Dashboard App with Mock Data

**Input**: Design documents from `/specs/004-dashboard-mock/`

**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: Tests are OPTIONAL - NOT included as feature specification does not explicitly request testing.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Dashboard app**: `apps/dashboard/src/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure

- [ ] T001 Create apps/dashboard directory structure per implementation plan
- [ ] T002 Initialize Vite 6 + React 19 + TypeScript 5.7 project in apps/dashboard/
- [ ] T003 Configure pnpm workspace dependency on @borne-map/ui in apps/dashboard/package.json
- [ ] T004 [P] Configure Tailwind CSS 4 in apps/dashboard/tailwind.config.js extending packages/ui tokens
- [ ] T005 [P] Configure TypeScript in apps/dashboard/tsconfig.json with strict mode and path aliases
- [ ] T006 [P] Configure Vite in apps/dashboard/vite.config.ts with React plugin and path aliases
- [ ] T007 [P] Create apps/dashboard/index.css with global styles and RTL support
- [ ] T008 [P] Create apps/dashboard/public/favicon.ico placeholder

**Checkpoint**: Project structure ready with build tools configured

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T009 Define TypeScript interfaces in apps/dashboard/src/types/index.ts (Partner, Station, Charger, User, Review, Report, UserRole)
- [ ] T010 Create mock partners.ts in apps/dashboard/src/mocks/partners.ts with 5 mock partner entities
- [ ] T011 Create mock stations.ts in apps/dashboard/src/mocks/stations.ts with 15 mock station entities (reuse from driver apps)
- [ ] T012 Create mock chargers.ts in apps/dashboard/src/mocks/chargers.ts with 50+ mock charger entities (reuse from driver apps)
- [ ] T013 Create mock users.ts in apps/dashboard/src/mocks/users.ts with 10 mock user entities
- [ ] T014 Create mock reviews.ts in apps/dashboard/src/mocks/reviews.ts with 60+ mock review entities (reuse from driver apps)
- [ ] T015 Create mock reports.ts in apps/dashboard/src/mocks/reports.ts with partner and admin report entities
- [ ] T016 Create i18n configuration in apps/dashboard/src/i18n/index.ts with react-i18next setup
- [ ] T017 Create French translations in apps/dashboard/src/i18n/fr.json with all static strings
- [ ] T018 Create Arabic translations in apps/dashboard/src/i18n/ar.json with all static strings
- [ ] T019 Create RoleContext in apps/dashboard/src/context/RoleContext.tsx with role state management
- [ ] T020 Create useRole hook in apps/dashboard/src/hooks/useRole.ts to access role context
- [ ] T021 Create useMockData hook in apps/dashboard/src/hooks/useMockData.ts to provide mock data access
- [ ] T022 Create useNavigation hook in apps/dashboard/src/hooks/useNavigation.ts for routing utilities
- [ ] T023 Configure React Router v7 in apps/dashboard/src/App.tsx with route structure

**Checkpoint**: Foundation ready - all mock data, types, i18n, routing, and role context in place

---

## Phase 3: User Story 1 - Partner Dashboard Navigation (Priority: P1) 🎯 MVP

**Goal**: Enable partners to navigate between Overview, My Stations, Station Edit, Charger Management, Availability Update, and Reports screens

**Independent Test**: Can be fully tested by logging in with mock partner role, verifying Overview displays correct metrics, and successfully navigating between all partner-specific screens

### Implementation for User Story 1

- [ ] T024 [P] [US1] Create AppShell in apps/dashboard/src/components/AppShell/AppShell.tsx with sidebar wrapper
- [ ] T025 [P] [US1] Create Sidebar in apps/dashboard/src/components/AppShell/Sidebar/Sidebar.tsx with navigation container
- [ ] T026 [P] [US1] Create BrandHeader in apps/dashboard/src/components/AppShell/Sidebar/BrandHeader.tsx with logo and title
- [ ] T027 [P] [US1] Create NavigationItem in apps/dashboard/src/components/AppShell/Sidebar/NavigationItem.tsx with icon, label, badge, active state
- [ ] T028 [P] [US1] Create BottomActions in apps/dashboard/src/components/AppShell/Sidebar/BottomActions.tsx with role toggle and logout
- [ ] T029 [US1] Create TopBar in apps/dashboard/src/components/AppShell/TopBar.tsx with tab navigation and user info
- [ ] T030 [P] [US1] Create PageContent in apps/dashboard/src/components/PageContent/PageContent.tsx with scrollable content area
- [ ] T031 [P] [US1] Create DataCard in apps/dashboard/src/components/DataCard/DataCard.tsx with CardHeader and body slot
- [ ] T032 [P] [US1] Create StatCard in apps/dashboard/src/components/StatCard/StatCard.tsx with value, label, trend indicator
- [ ] T033 [P] [US1] Create DataTable in apps/dashboard/src/components/DataTable/DataTable.tsx with sorting, pagination, row actions
- [ ] T034 [P] [US1] Create table types in apps/dashboard/src/components/DataTable/table.types.ts for column definitions
- [ ] T035 [US1] Create OverviewScreen in apps/dashboard/src/screens/OverviewScreen.tsx with 4 stat cards and station data card
- [ ] T036 [US1] Create MyStationsScreen in apps/dashboard/src/screens/MyStationsScreen.tsx with station data table
- [ ] T037 [US1] Create StationEditScreen in apps/dashboard/src/screens/StationEditScreen.tsx with static form fields
- [ ] T038 [US1] Create ChargerManagementScreen in apps/dashboard/src/screens/ChargerManagementScreen.tsx with charger data table
- [ ] T039 [US1] Create AvailabilityUpdateScreen in apps/dashboard/src/screens/AvailabilityUpdateScreen.tsx with toggle controls
- [ ] T040 [US1] Create ReportsScreen in apps/dashboard/src/screens/ReportsScreen.tsx with 4 stat cards and chart placeholders
- [ ] T041 [US1] Update App.tsx in apps/dashboard/src/App.tsx to render AppShell with partner-specific routes
- [ ] T042 [US1] Update RoleContext in apps/dashboard/src/context/RoleContext.tsx to hide non-partner navigation items

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently

---

## Phase 4: User Story 2 - Admin Dashboard Navigation (Priority: P1)

**Goal**: Enable admins to navigate between Overview, Users, Partners, Stations, Chargers, Reviews, and Reports screens

**Independent Test**: Can be fully tested by logging in with mock admin role, verifying Overview displays comprehensive platform metrics, and successfully navigating between all admin-specific screens

### Implementation for User Story 2

- [ ] T043 [US2] Create UsersScreen in apps/dashboard/src/screens/UsersScreen.tsx with user data table
- [ ] T044 [US2] Create PartnersScreen in apps/dashboard/src/screens/PartnersScreen.tsx with partner data table
- [ ] T045 [US2] Create Admin OverviewScreen in apps/dashboard/src/screens/OverviewScreen.tsx with 6 stat cards, live station list, active drivers table
- [ ] T046 [US2] Create Admin ReportsScreen in apps/dashboard/src/screens/ReportsScreen.tsx with 6 stat cards and chart placeholders
- [ ] T047 [US2] Update App.tsx in apps/dashboard/src/App.tsx to render admin-specific routes
- [ ] T048 [US2] Update RoleContext in apps/dashboard/src/context/RoleContext.tsx to hide non-admin navigation items
- [ ] T049 [US2] Update OverviewScreen in apps/dashboard/src/screens/OverviewScreen.tsx to display different content based on role

**Checkpoint**: At this point, User Stories 1 AND 2 should both work independently

---

## Phase 5: User Story 3 - Language Switching and RTL Support (Priority: P2)

**Goal**: Enable users to switch between Arabic (RTL) and French languages with proper RTL layout on all screens

**Independent Test**: Can be fully tested by switching to Arabic on any screen and verifying sidebar aligns right, tables format correctly, and forms align properly

### Implementation for User Story 3

- [ ] T050 [P] [US3] Update apps/dashboard/src/components/AppShell/Sidebar/Sidebar.tsx to align right in Arabic RTL layout
- [ ] T051 [P] [US3] Update apps/dashboard/src/components/DataTable/DataTable.tsx to format tables correctly in RTL layout
- [ ] T052 [P] [US3] Update apps/dashboard/src/screens/StationEditScreen.tsx to align form elements correctly in RTL layout
- [ ] T053 [US3] Update apps/dashboard/src/i18n/index.ts to set dir="rtl" on HTML element when Arabic is selected
- [ ] T054 [US3] Update Tailwind config in apps/dashboard/tailwind.config.js to support RTL modifiers (rtl:ml-4, etc.)
- [ ] T055 [US3] Update AppShell in apps/dashboard/src/components/AppShell/AppShell.tsx to re-render on language change
- [ ] T056 [US3] Update OverviewScreen in apps/dashboard/src/screens/OverviewScreen.tsx to accommodate longer Arabic text in cards
- [ ] T057 [US3] Update all screen components in apps/dashboard/src/screens/ to use CSS logical properties (margin-inline-start, padding-inline-end)

**Checkpoint**: RTL layout works correctly on all screens in Arabic

---

## Phase 6: User Story 4 - Development Role Switching (Priority: P2)

**Goal**: Enable developers to toggle between partner and admin roles via dev-only UI control for testing

**Independent Test**: Can be fully tested by clicking the role toggle and verifying navigation menu and screen content changes between partner and admin views

### Implementation for User Story 4

- [ ] T058 [US4] Update BottomActions in apps/dashboard/src/components/AppShell/Sidebar/BottomActions.tsx to add dev-only role toggle button
- [ ] T059 [US4] Update RoleContext in apps/dashboard/src/context/RoleContext.tsx to handle role toggle from BottomActions
- [ ] T060 [US4] Update useNavigation hook in apps/dashboard/src/hooks/useNavigation.ts to redirect to Overview when switching roles
- [ ] T061 [US4] Update App.tsx in apps/dashboard/src/App.tsx to ensure AppShell re-renders on role change
- [ ] T062 [US4] Style role toggle in apps/dashboard/src/components/AppShell/Sidebar/BottomActions.tsx to be clearly distinguishable as dev-only

**Checkpoint**: Role switching completes within 1 second and correctly updates UI

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [ ] T063 [P] Update apps/dashboard/src/components/EmptyState (if needed) with design token values
- [ ] T064 [P] Verify all visual values in apps/dashboard/src/components/ consume tokens from packages/ui
- [ ] T065 [P] Verify all visual values in apps/dashboard/src/screens/ consume tokens from packages/ui
- [ ] T066 [P] Update apps/dashboard/src/components/DataTable/DataTable.tsx to handle empty data arrays
- [ ] T067 [P] Update apps/dashboard/src/screens/ to handle empty state messages when mock data is empty
- [ ] T068 Run TypeScript type check in apps/dashboard/ with zero errors
- [ ] T069 Run Vite build in apps/dashboard/ with zero warnings
- [ ] T070 Verify Overview screen loads within 3 seconds on initial page load
- [ ] T071 Verify role switching completes within 1 second

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Stories (Phase 3-6)**: All depend on Foundational phase completion
  - User stories can then proceed in parallel (if staffed)
  - Or sequentially in priority order (P1 → P2)
- **Polish (Phase 7)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 2 (P2)**: Can start after Foundational (Phase 2) - Shares OverviewScreen with US1 but different admin variant
- **User Story 3 (P2)**: Can start after Foundational (Phase 2) - Applies RTL fixes to all existing components from US1 and US2
- **User Story 4 (P2)**: Can start after Foundational (Phase 2) - Adds role toggle functionality to existing components

### Within Each User Story

- Components can be built in parallel if marked [P]
- Screens depend on components being available
- Core implementation before integration
- Story complete before moving to next priority

### Parallel Opportunities

- All Setup tasks marked [P] can run in parallel
- All Foundational mock data tasks (T010-T015) can run in parallel
- All Dashboard components (T024-T034) can run in parallel
- All Partner screens (T035-T040) can run in parallel after components
- US3 RTL fixes (T050-T052) can run in parallel
- Polish verification tasks (T064-T067) can run in parallel

---

## Parallel Example: User Story 1

```bash
# Launch all dashboard components together:
Task: "Create AppShell in apps/dashboard/src/components/AppShell/AppShell.tsx with sidebar wrapper"
Task: "Create Sidebar in apps/dashboard/src/components/AppShell/Sidebar/Sidebar.tsx with navigation container"
Task: "Create BrandHeader in apps/dashboard/src/components/AppShell/Sidebar/BrandHeader.tsx with logo and title"
Task: "Create NavigationItem in apps/dashboard/src/components/AppShell/Sidebar/NavigationItem.tsx with icon, label, badge, active state"
Task: "Create BottomActions in apps/dashboard/src/components/AppShell/Sidebar/BottomActions.tsx with role toggle and logout"
Task: "Create TopBar in apps/dashboard/src/components/AppShell/TopBar.tsx with tab navigation and user info"
Task: "Create PageContent in apps/dashboard/src/components/PageContent/PageContent.tsx with scrollable content area"
Task: "Create DataCard in apps/dashboard/src/components/DataCard/DataCard.tsx with CardHeader and body slot"
Task: "Create StatCard in apps/dashboard/src/components/StatCard/StatCard.tsx with value, label, trend indicator"
Task: "Create DataTable in apps/dashboard/src/components/DataTable/DataTable.tsx with sorting, pagination, row actions"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL - blocks all stories)
3. Complete Phase 3: User Story 1
4. **STOP and VALIDATE**: Test User Story 1 independently with mock partner role
5. Verify all 6 partner screens display with mock data

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 → Test independently → Validate Partner Dashboard
3. Add User Story 2 → Test independently → Validate Admin Dashboard
4. Add User Story 3 → Test independently → Validate RTL Support
5. Add User Story 4 → Test independently → Validate Role Switching
6. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1 (Partner Dashboard Navigation)
   - Developer B: User Story 2 (Admin Dashboard Navigation)
   - Developer C: User Story 3 (RTL Support)
3. Stories complete and integrate independently

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- All visual values must consume tokens from packages/ui (constitutional requirement)
- RTL failures are Class A bugs - verify carefully
- Role switching must complete within 1 second (SC-004)
- No backend calls - all data from local mock files
- Stop at any checkpoint to validate story independently
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break independence