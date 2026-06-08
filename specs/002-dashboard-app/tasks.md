# Tasks: Dashboard App

**Input**: Design documents from `/specs/002-dashboard-app/`

**Prerequisites**: plan.md (✅ complete), spec.md (✅ complete), data-model.md (✅ complete), contracts/api-integration.md (✅ complete), quickstart.md (✅ complete)

**Tests**: Tests are OPTIONAL. This task list focuses on implementation tasks (no test tasks generated). E2E testing covered separately in polish phase.

**Organization**: Tasks grouped by user story (P1, P2) to enable independent implementation of each story. All stories depend on foundational infrastructure (Phase 2).

---

## Format: `[ID] [P?] [Story] Description`

- **Checkbox**: `- [ ]` (markdown task)
- **ID**: Sequential T001, T002, T003...
- **[P]**: Can run in parallel (different files, no dependencies on incomplete tasks)
- **[Story]**: User story label [US1], [US2], [US3], [US4] (setup/foundation phases have no label)
- **Description**: Clear action with exact file path

**Example**: `- [ ] T005 [P] Create API client in src/services/api.ts`

---

## User Story Dependencies

```
┌─────────────────────────────────────────────────────────────┐
│ Phase 2: Foundational (BLOCKING - must complete first)      │
│ ├─ AppShell layout + routing                                 │
│ ├─ API client + error handling                              │
│ ├─ Design tokens import + Tailwind config                   │
│ ├─ Custom data hooks (usePartners, useStations, useChargers)│
│ ├─ Form validation utilities                                │
│ └─ Common components (Modal, DataTable, ErrorState, etc.)   │
│                                                              │
│ ↓                                                             │
│ Phase 3a: User Story 1 (P1) - Partner Management [P]        │
│ ├─ PartnerForm component                                    │
│ ├─ PartnersScreen component                                 │
│ └─ Integrate usePartners hook                               │
│                                                              │
│ ↓                                                             │
│ Phase 3b: User Story 2 (P1) - Station Management [P]        │
│ ├─ StationForm component (with coord validation)            │
│ ├─ StationsScreen component + filter dropdown              │
│ └─ Integrate useStations hook                               │
│                                                              │
│ ↓                                                             │
│ Phase 3c: User Story 3 (P1) - Charger Management [P]        │
│ ├─ ChargerForm component (with status enum)                 │
│ ├─ ChargersScreen component + filter dropdown              │
│ └─ Integrate useChargers hook                               │
│                                                              │
│ ↓                                                             │
│ Phase 4: User Story 4 (P2) - Overview Dashboard             │
│ ├─ StatCard component                                       │
│ ├─ Overview screen                                          │
│ └─ Fetch real counts from API                              │
│                                                              │
│ ↓                                                             │
│ Phase 5: Polish & Cross-Cutting Concerns                    │
│ ├─ Loading states + skeleton loaders                        │
│ ├─ Error handling refinement                                │
│ ├─ Empty state polish                                       │
│ ├─ Cross-browser testing (Chrome, Firefox, Safari)          │
│ ├─ Performance optimization                                 │
│ └─ Documentation & deployment prep                          │
└─────────────────────────────────────────────────────────────┘
```

**Key**: User Stories 1, 2, 3 (Partner, Station, Charger) can proceed in parallel after Phase 2 is complete.

---

## Phase 1: Setup (Project Initialization)

**Purpose**: Initialize React + Vite + TypeScript project with dependencies and project structure

**Duration**: ~2-3 hours

- [ ] T001 Initialize Vite + React project at `source/apps/dashboard/` with TypeScript template per quickstart.md
- [ ] T002 Install core dependencies: react, react-router-dom, axios, react-hook-form, @headlessui/react, classnames in `source/apps/dashboard/`
- [ ] T003 Install dev dependencies: TypeScript, @types/react, @types/node, Vite, Tailwind CSS, PostCSS, autoprefixer
- [ ] T004 [P] Create directory structure: `src/{components,services,hooks,types,utils}` and `tests/{unit,integration}` in `source/apps/dashboard/`
- [ ] T005 [P] Configure Tailwind CSS with design token colors in `source/apps/dashboard/tailwind.config.js` extending shared base
- [ ] T006 [P] Configure PostCSS in `source/apps/dashboard/postcss.config.js`
- [ ] T007 [P] Create TypeScript configuration in `source/apps/dashboard/tsconfig.json` with path aliases
- [ ] T008 Create Vite configuration in `source/apps/dashboard/vite.config.ts` with React plugin and environment variables
- [ ] T009 [P] Create environment template in `source/apps/dashboard/.env.example` with VITE_API_BASE_URL
- [ ] T010 Create global styles in `source/apps/dashboard/src/index.css` importing Tailwind directives
- [ ] T011 Create HTML template in `source/apps/dashboard/index.html` with root div

**Checkpoint**: Project structure created, dependencies installed, Tailwind configured with design tokens

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure MUST be complete before ANY user story work

**⚠️ CRITICAL**: No user story implementation can begin until Phase 2 is complete

**Duration**: ~3-4 hours

### 2a. API Client & Error Handling

- [ ] T012 Create API client in `src/services/api.ts` using axios with base URL and error interceptor per contracts/api-integration.md
- [ ] T013 [P] Create Partner API service in `src/services/partners.ts` with CRUD functions (GET, POST, PUT, DELETE /api/v1/partners)
- [ ] T014 [P] Create Station API service in `src/services/stations.ts` with CRUD + filter (GET, POST, PUT, DELETE /api/v1/stations)
- [ ] T015 [P] Create Charger API service in `src/services/chargers.ts` with CRUD + filter (GET, POST, PUT, DELETE /api/v1/chargers)

### 2b. TypeScript Types & Models

- [ ] T016 [P] Create API response types in `src/types/api.ts` with Partner, Station, Charger, ChargerStatus interfaces per data-model.md
- [ ] T017 [P] Create form payload types in `src/types/forms.ts` with Create/Update payloads for Partner, Station, Charger

### 2c. Data Fetching Hooks

- [ ] T018 Create `usePartners` hook in `src/hooks/usePartners.ts` with fetch, create, update, delete methods per data-model.md
- [ ] T019 [P] Create `useStations` hook in `src/hooks/useStations.ts` with CRUD + partner filter capability
- [ ] T020 [P] Create `useChargers` hook in `src/hooks/useChargers.ts` with CRUD + station filter capability
- [ ] T021 Create `useForm` hook in `src/hooks/useForm.ts` wrapper around React Hook Form for common submit handling

### 2d. Validation Utilities

- [ ] T022 [P] Create validation module in `src/utils/validation.ts` with functions: validatePartnerName, validateAddress, validateLatitude, validateLongitude, validateConnectorType, validatePowerKw, validateStatus
- [ ] T023 [P] Create error message helpers in `src/utils/errors.ts` for API error → user message mapping

### 2e. AppShell Layout

- [ ] T024 Create Sidebar component in `src/components/AppShell/Sidebar.tsx` with fixed width (w-64), white bg, nav items, active state styling
- [ ] T025 Create TopBar component in `src/components/AppShell/TopBar.tsx` with fixed height (h-16), brand name, border-bottom
- [ ] T026 Create Layout wrapper in `src/components/AppShell/Layout.tsx` combining Sidebar + TopBar + scrollable main content
- [ ] T027 Create App.tsx with React Router setup: routes for /, /partners, /stations, /chargers per plan.md

### 2f. Common Components

- [ ] T028 [P] Create Modal component in `src/components/Common/Modal.tsx` as form wrapper with overlay, centered card, close, submit/cancel buttons
- [ ] T029 [P] Create DataTable component in `src/components/Common/DataTable.tsx` as reusable table with headers, rows, actions column (edit/delete)
- [ ] T030 [P] Create StatCard component in `src/components/Common/StatCard.tsx` for displaying label, count, optional icon
- [ ] T031 [P] Create ErrorState component in `src/components/Common/ErrorState.tsx` for API unreachable with retry button
- [ ] T032 [P] Create EmptyState component in `src/components/Common/EmptyState.tsx` for empty data with "Create" prompt
- [ ] T033 [P] Create LoadingSkeletons component in `src/components/Common/LoadingSkeletons.tsx` for table/card loaders
- [ ] T034 [P] Create StatusBadge component in `src/components/Common/StatusBadge.tsx` with colors: available (green), in_use (amber), maintenance (red) from tokens

### 2g. Root Component Setup

- [ ] T035 Create main.tsx entry point in `src/main.tsx` rendering App component
- [ ] T036 Create placeholder screens in `src/components/Screens/`: Overview.tsx, PartnersScreen.tsx, StationsScreen.tsx, ChargersScreen.tsx (empty content)

**Checkpoint**: Foundation complete - all data hooks, API client, components, and routing in place. Ready for user story implementation.

---

## Phase 3a: User Story 1 - Partner Management (Priority: P1) 🎯 MVP

**Goal**: Partner managers can create, read, update, and delete partner records through the Dashboard. Partners are the foundation for all other entities.

**Independent Test**: 
1. Navigate to `/partners` screen
2. Click "Create Partner" button
3. Enter partner name "Test Partner Co"
4. Click "Save" and verify it appears in the table
5. Click "Edit" on the partner
6. Change name to "Updated Partner Co"
7. Click "Save" and verify table updates
8. Click "Delete" and confirm
9. Verify partner is removed from table

**Duration**: ~4 hours

### Implementation for User Story 1

- [ ] T037 [P] Create PartnerForm component in `src/components/Forms/PartnerForm.tsx` with: name input, validation error display, submit/cancel buttons, form submission handling
- [ ] T038 [P] Create PartnersScreen component in `src/components/Screens/PartnersScreen.tsx` with: DataTable, Create button opening modal, Edit action, Delete action with confirmation
- [ ] T039 Integrate `usePartners` hook into PartnersScreen: fetch on mount, handle loading/error states, update table on mutations (create/update/delete)
- [ ] T040 [P] Add inline form validation: trigger on field blur, show error messages below field in PartnerForm
- [ ] T041 Add error handling for API failures in PartnersScreen: show ErrorState if fetch fails, show field errors on 422, retry on network errors
- [ ] T042 [P] Add loading skeleton while fetching partners list in PartnersScreen
- [ ] T043 [P] Add empty state to PartnersScreen when no partners exist: "No partners yet. Create one to get started."
- [ ] T044 Add success feedback: table updates immediately on create/update/delete without page reload (optimistic updates)

**Checkpoint**: User Story 1 fully functional - partners can be created, listed, edited, and deleted independently

---

## Phase 3b: User Story 2 - Station Management (Priority: P1) 🎯 MVP

**Goal**: Partner managers can create, read, update, and delete stations with geographic coordinates and partner associations. Stations are locations where chargers operate.

**Independent Test**:
1. Navigate to `/stations` screen
2. Verify partners dropdown shows all available partners
3. Click "Create Station" button
4. Enter: name "Tunis Central", address "Ave Bourguiba", latitude 36.8065, longitude 10.1963, select partner
5. Click "Save" and verify it appears in table with partner name and charger count
6. Enter invalid latitude 91 and verify validation error
7. Edit a station's address and verify update
8. Delete a station and verify removal

**Duration**: ~5 hours

### Implementation for User Story 2

- [ ] T045 [P] Create StationForm component in `src/components/Forms/StationForm.tsx` with: name input, address input, latitude input, longitude input, partner dropdown, validation error display
- [ ] T046 [P] Create StationsScreen component in `src/components/Screens/StationsScreen.tsx` with: DataTable showing name, address, partner name, charger count; Create button; Edit/Delete actions; Partner filter dropdown
- [ ] T047 Integrate `useStations` hook into StationsScreen: fetch on mount, apply partner filter, handle loading/error states, update on mutations
- [ ] T048 [P] Populate partner dropdown in StationForm: fetch partners on form mount, set partner_id in payload
- [ ] T049 [P] Populate partner filter dropdown in StationsScreen: fetch partners on screen mount, refetch stations when filter changes
- [ ] T050 [P] Add coordinate validation in StationForm: latitude (-90 to 90), longitude (-180 to 180), show inline errors
- [ ] T051 [P] Add address validation in StationForm: required, max 500 chars
- [ ] T052 [P] Add address validation in StationForm: required, max 255 chars
- [ ] T053 Add form validation triggering on blur in StationForm
- [ ] T054 [P] Add loading skeleton while fetching stations list in StationsScreen
- [ ] T055 [P] Add empty state to StationsScreen when no stations exist
- [ ] T056 [P] Add error handling for API failures: show ErrorState on fetch failure, field errors on 422, network error messages
- [ ] T057 Add success feedback: table updates immediately without reload, show charger count correctly

**Checkpoint**: User Story 2 fully functional - stations with location validation can be managed independently

---

## Phase 3c: User Story 3 - Charger Management (Priority: P1) 🎯 MVP

**Goal**: Partner managers can create, read, update (especially status), and delete chargers at specific stations. Chargers are the atomic units that drivers interact with.

**Independent Test**:
1. Navigate to `/chargers` screen
2. Verify stations dropdown shows all available stations
3. Click "Create Charger" button
4. Enter: station selection, connector type "Type2", power 22, status "available"
5. Click "Save" and verify it appears with status badge (green for available)
6. Click "Edit" on charger and change status to "maintenance"
7. Verify badge color changes to red
8. Delete a charger and verify removal

**Duration**: ~5 hours

### Implementation for User Story 3

- [ ] T058 [P] Create ChargerForm component in `src/components/Forms/ChargerForm.tsx` with: station dropdown, connector type select (Type2, CCS, CHAdeMO, J1772), power kW input, status select (available, in_use, maintenance)
- [ ] T059 [P] Create ChargersScreen component in `src/components/Screens/ChargersScreen.tsx` with: DataTable showing station name, connector type, power kW, status badge; Create button; Edit/Delete actions; Station filter dropdown
- [ ] T060 Integrate `useChargers` hook into ChargersScreen: fetch on mount, apply station filter, handle loading/error states, update on mutations
- [ ] T061 [P] Populate station dropdown in ChargerForm: fetch stations on form mount, set station_id in payload
- [ ] T062 [P] Populate station filter dropdown in ChargersScreen: fetch stations on screen mount, refetch chargers when filter changes
- [ ] T063 [P] Add connector type validation in ChargerForm: required, must be one of enum values
- [ ] T064 [P] Add power kW validation in ChargerForm: required, positive number
- [ ] T065 [P] Add status validation in ChargerForm: required, one of [available, in_use, maintenance]
- [ ] T066 [P] Render StatusBadge in ChargersScreen DataTable status column with correct colors per data-model.md
- [ ] T067 [P] Add loading skeleton while fetching chargers list in ChargersScreen
- [ ] T068 [P] Add empty state to ChargersScreen when no chargers exist
- [ ] T069 [P] Add error handling for API failures: show ErrorState on fetch failure, field errors on 422, network error messages
- [ ] T070 Add success feedback: table updates immediately without reload, badge color reflects status changes

**Checkpoint**: User Story 3 fully functional - chargers with status badges can be managed independently

---

## Phase 4: User Story 4 - Overview Dashboard (Priority: P2)

**Goal**: Dashboard summary with real-time stats showing total partners, stations, and chargers in the system.

**Independent Test**:
1. Navigate to `/` (Overview screen)
2. Verify three StatCards display: total partners, total stations, total chargers
3. Counts match actual data in database
4. Create a partner and return to Overview
5. Verify partner count increases (or reloads data)

**Duration**: ~2 hours

### Implementation for User Story 4

- [ ] T071 Create Overview screen in `src/components/Screens/Overview.tsx` with three StatCards
- [ ] T072 [P] Integrate StatCards to display real partner count: fetch from `/api/v1/partners`, show count in StatCard
- [ ] T073 [P] Integrate StatCards to display real station count: fetch from `/api/v1/stations`, show count in StatCard
- [ ] T074 [P] Integrate StatCards to display real charger count: fetch from `/api/v1/chargers`, show count in StatCard
- [ ] T075 [P] Add loading skeleton to StatCards while fetching counts
- [ ] T076 [P] Add error state to StatCards if fetch fails

**Checkpoint**: User Story 4 complete - Overview screen shows real stats from API

---

## Phase 5: Polish & Cross-Cutting Concerns

**Purpose**: Refine UX, ensure consistency, optimize performance, test cross-browser compatibility, prepare for deployment

**Duration**: ~3-4 hours

### 5a. Loading & Empty State Polish

- [ ] T077 [P] Review all loading skeletons across 4 screens: Partners, Stations, Chargers, Overview; ensure consistent styling from tokens
- [ ] T078 [P] Review all empty states across 4 screens: verify messaging, styling, "Create" button visibility
- [ ] T079 [P] Add skeleton loaders to form modals while loading dropdown options (partner, station, connector type)

### 5b. Error Handling Refinement

- [ ] T080 [P] Add retry logic to ErrorState component: retry button refetches data
- [ ] T081 [P] Test all error scenarios: network down, API 500, 404, validation 422, timeouts
- [ ] T082 [P] Verify error messages are user-friendly (no console error details exposed)
- [ ] T083 [P] Add form error persistence: when submit fails, form data preserved for correction

### 5c. Performance Optimization

- [ ] T084 Audit DataTable renders: verify no unnecessary re-renders on mutation
- [ ] T085 Optimize API calls: ensure no duplicate fetches on mount
- [ ] T086 Verify page load time < 3 seconds with full data fetch

### 5d. Cross-Browser Testing

- [ ] T087 Test on Chrome (latest): verify all screens render, CRUD operations work, no console errors
- [ ] T088 Test on Firefox (latest): verify layout, forms, modals, dropdown styling
- [ ] T089 Test on Safari (latest): verify responsive layout, form inputs, button interactions
- [ ] T090 Document any browser-specific styling fixes applied to `src/index.css` or component files

### 5e. Accessibility & UX Polish

- [ ] T091 [P] Verify form labels are associated with inputs (htmlFor attributes)
- [ ] T092 [P] Verify modal close button is keyboard accessible (Tab, Escape key)
- [ ] T093 [P] Verify DataTable action buttons (Edit, Delete) are accessible
- [ ] T094 [P] Test tab navigation through entire Dashboard

### 5f. Documentation & Deployment Prep

- [ ] T095 Create or update README.md in `source/apps/dashboard/` with: setup instructions, running dev server, building for production
- [ ] T096 Add JSDoc comments to: API client functions, hooks, validation utilities
- [ ] T097 Create `.gitignore` in `source/apps/dashboard/` excluding node_modules, dist, .env.local
- [ ] T098 Verify package.json scripts: `npm run dev`, `npm run build`, `npm run preview` (production preview)

### 5g. Full Loop Verification

- [ ] T099 End-to-end test sequence: Create partner → Create station for that partner → Create charger for that station → Edit charger status → Verify in Driver Web app (if ready)
- [ ] T100 Verify data consistency: Create entity in Dashboard → fetch in API → verify in database
- [ ] T101 Test with backend unreachable: Dashboard shows ErrorState gracefully, no crashes
- [ ] T102 Test with seeded data: 3 partners, 15 stations, 24 chargers all visible and filterable

**Checkpoint**: Dashboard fully polished, cross-browser tested, ready for staging/production

---

## Task Summary

**Total Tasks**: 102

**By Phase**:
- Phase 1 (Setup): 11 tasks
- Phase 2 (Foundational): 25 tasks
- Phase 3a (Partner CRUD - US1): 8 tasks
- Phase 3b (Station CRUD - US2): 12 tasks
- Phase 3c (Charger CRUD - US3): 13 tasks
- Phase 4 (Overview - US4): 6 tasks
- Phase 5 (Polish): 27 tasks

**Parallelizable Tasks**: 54 marked with [P] (can run in parallel within each phase/story)

---

## Implementation Strategy

### MVP Scope (Sprint 1.2 - 2 weeks)

**Must Have (P1 - User Stories 1, 2, 3)**:
- Partner CRUD (US1)
- Station CRUD with location validation (US2)
- Charger CRUD with status management (US3)
- All 4 screens functional with real API data
- Form validation + error handling
- Loading states + empty states

**Nice to Have (P2 - User Story 4)**:
- Overview screen with real stats
- Can defer to Sprint 1.3 if time is tight

### Recommended Execution Order

1. **Week 1**:
   - Phase 1: Setup (1 day)
   - Phase 2: Foundational (1.5 days)
   - Phase 3a: Partner CRUD (1 day)
   - Phase 3b: Station CRUD (1.5 days)

2. **Week 2**:
   - Phase 3c: Charger CRUD (1.5 days)
   - Phase 4: Overview (0.5 days)
   - Phase 5: Polish + testing (1.5 days)

### Parallel Work Opportunities

**Within each phase**, many [P] tasks can run in parallel:

- **Phase 2**: API services (T013-T015), types (T016-T017), hooks (T019-T020), validation (T022-T023), common components (T028-T034) can all be parallelized
- **Phase 3a-c**: Different form components (PartnerForm, StationForm, ChargerForm) can be built in parallel with screens

### Verification Points (Checkpoints)

After each phase, verify:
- ✅ Phase 1: Dev server runs (`npm run dev`), no TypeScript errors
- ✅ Phase 2: API client works (test health endpoint), hooks return data, components render
- ✅ Phase 3a: Partners CRUD fully testable, empty/loading states work
- ✅ Phase 3b: Stations CRUD fully testable, filtering by partner works, validation prevents invalid coords
- ✅ Phase 3c: Chargers CRUD fully testable, status badges show correct colors
- ✅ Phase 4: Overview shows real counts
- ✅ Phase 5: All screens work on Chrome/Firefox/Safari, API down gracefully handled, no console errors

---

## Dependencies Map

```
T001-T011 (Phase 1 Setup)
    ↓
T012-T036 (Phase 2 Foundational - BLOCKING)
    ├─ T037-T044 (Phase 3a - Partner CRUD) ← Can start once T012-T036 done
    ├─ T045-T057 (Phase 3b - Station CRUD) ← Can start once T012-T036 done
    └─ T058-T070 (Phase 3c - Charger CRUD) ← Can start once T012-T036 done
            ↓
        T071-T076 (Phase 4 - Overview)
            ↓
        T077-T102 (Phase 5 - Polish)
```

**Key insight**: Phases 3a, 3b, 3c can proceed **in parallel** after Phase 2 is complete.

---

## Success Criteria (Definition of Done)

- ✅ All 102 tasks have checkboxes marked complete
- ✅ No console errors in Chrome DevTools
- ✅ All 4 screens load and display data
- ✅ Full CRUD workflow tested: Partner → Station → Charger → Edit → Delete
- ✅ Form validation prevents invalid data submission
- ✅ API unreachable handled gracefully
- ✅ Cross-browser testing on Chrome, Firefox, Safari completed
- ✅ Performance: Page load < 3 seconds, mutations update table < 1 second
- ✅ Design tokens used throughout (no hardcoded colors)
- ✅ Ready for handoff to driver apps integration

---

**Ready to implement! 🚀**
