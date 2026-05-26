# Tasks: Mobile Driver App — Map Discovery

**Input**: Design documents from `specs/006-mobile-driver-app/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/api.md

**Tests**: Not requested in feature specification — no test tasks generated.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Mobile app**: `sources/frontend/apps/mobile-driver/`
- **Backend**: `sources/backend/src/`
- All paths below are relative to `sources/frontend/` or `sources/backend/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization — verify existing build and install new dependencies.

- [X] T001 Verify frontend build — build passes cleanly
- [X] T002 Add mobile-specific dependencies — expo-location (already present), react-native-maps, @gorhom/bottom-sheet, react-native-reanimated, react-native-gesture-handler, expo-haptics, @react-native-async-storage/async-storage
- [X] T003 Make nearby endpoint public — already public (no JWT required)

**Checkpoint**: Frontend builds with new deps, nearby endpoint accepts anonymous requests

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Expo project structure, routing, and API client — MUST be complete before ANY user story can render map content.

- [X] T004 [P] Create TypeScript types in `apps/mobile-driver/src/types/station.ts`
- [X] T005 [P] Create API client in `apps/mobile-driver/src/services/nearby-api.ts`
- [X] T006 Create location service in `apps/mobile-driver/src/services/location.ts`

**Checkpoint**: API client fetches data, location service requests permission, types are defined

---

## Phase 3: User Story 1 — Driver discovers nearby stations on a map (Priority: P1) 🎯 MVP

**Goal**: Full-viewport map centered on Tunisia with station markers. On location grant, re-centers to user and fetches nearby stations.

**Independent Test**: Open the app — full-viewport map appears centered on Tunisia. Grant location — map re-centers and station markers appear within 20km.

### Implementation for User Story 1

- [X] T007 [US1] Create full-viewport StationMap component in `apps/mobile-driver/src/components/map/station-map.tsx`
- [X] T008 [US1] Implement location-based centering with error/empty states in station-map.tsx
- [X] T009 [US1] Wire map screen in `apps/mobile-driver/app/index.tsx`
- [ ] T010 [US1] Add marker clustering — requires react-native-maps cluster prop or additional library

**Checkpoint**: Map renders, markers appear, clustering works for >20 results, empty/error states handled

---

## Phase 4: User Story 2 — Driver views station details in a bottom sheet (Priority: P1)

**Goal**: Tapping a station marker opens a bottom sheet with station name, address, distance, available charger count, and charger list.

**Independent Test**: Tap a station marker — bottom sheet slides up with station details and charger list. Swipe down to dismiss. Tap another marker — sheet updates with new station's details.

### Implementation for User Story 2

- [X] T011 [P] [US2] Create StationSheet component in `apps/mobile-driver/src/components/station-sheet.tsx`
- [X] T012 [P] [US2] Add charger list section to StationSheet with status badges
- [X] T013 [US2] Wire marker tap → sheet in `app/index.tsx` via onStationSelect

**Checkpoint**: Bottom sheet opens on marker tap, shows station and charger details, dismisses on swipe, updates on new marker

---

## Phase 5: User Story 3 — Driver navigates to a station (Priority: P2)

**Goal**: "Navigate" button in the bottom sheet opens device maps app with station coordinates.

**Independent Test**: Tap station marker → bottom sheet → tap "Navigate" → device maps app opens with station as destination.

### Implementation for User Story 3

- [X] T014 [US3] Add "Navigate" button to StationSheet with Linking.openURL

**Checkpoint**: Tapping "Navigate" opens device maps with station coordinates

---

## Phase 6: User Story 4 — Driver adjusts search radius (Priority: P2)

**Goal**: Radius slider (5/10/20/50km) and pull-to-refresh re-fetch nearby stations.

**Independent Test**: Change radius from 20km to 50km — new markers appear further away. Pull to refresh — markers re-fetch.

### Implementation for User Story 4

- [ ] T015 [US4] Add radius selector overlay in `apps/mobile-driver/src/components/map/station-map.tsx` — row of 4 pill buttons (5km, 10km, 20km, 50km); selected radius highlighted; changing radius re-fetches nearby stations
- [ ] T016 [US4] Store radius preference in `apps/mobile-driver/src/services/nearby-api.ts` — persist selected radius via `@react-native-async-storage/async-storage`; restore on app launch
- [ ] T017 [US4] Implement pull-to-refresh on map — wrap MapView in a pull-to-refresh container; on refresh, re-fetch nearby stations using current map center and selected radius

**Checkpoint**: Radius selector visible, changing radius re-fetches stations, preference persists across app restarts, pull-to-refresh works

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Verification and cleanup across all stories

- [ ] T018 [P] Run type-check — `pnpm -r type-check` in `sources/frontend/`
- [ ] T019 [P] Run lint — `pnpm -r lint` in `sources/frontend/`
- [ ] T020 [P] Run build — `pnpm -r build` in `sources/frontend/`
- [ ] T021 Verify backend compiles — `cargo build` in `sources/backend/`
- [ ] T022 Run quickstart validation — execute all verification steps in `specs/006-mobile-driver-app/quickstart.md`

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **US1 — Map Discovery (Phase 3)**: Depends on Foundational completion — BLOCKS US2 and US4 (US2 needs markers, US4 needs map)
- **US2 — Detail Sheet (Phase 4)**: Depends on US1 completion (needs markers to tap)
- **US3 — Navigation (Phase 5)**: Depends on US2 completion (needs bottom sheet with Navigate button)
- **US4 — Radius & Refresh (Phase 6)**: Depends on US1 completion (needs map to adjust radius on)
- **Polish (Phase 7)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: After Foundational — BLOCKS US2, US4
- **User Story 2 (P1)**: After US1 — BLOCKS US3
- **User Story 3 (P2)**: After US2 — No dependencies on other stories
- **User Story 4 (P2)**: After US1 — No dependencies on other stories

### Within Each User Story

- Parallel tasks marked [P] can be executed concurrently
- Non-[P] tasks are sequential within each phase
- Phases should complete before moving to next phase due to story dependencies

### Parallel Opportunities

- T004, T005 (Foundational) can run in parallel — types + API client
- T011, T012 (US2) can run in parallel — sheet component + charger list

---

## Parallel Example: Phase 2 Foundational

```bash
# Launch all Foundational tasks together:
Task: "Create TypeScript types in src/types/station.ts"
Task: "Create API client in src/services/nearby-api.ts"
```

## Parallel Example: User Story 2

```bash
# Launch parallel US2 tasks together:
Task: "Create StationSheet component"
Task: "Add charger list section to StationSheet"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational
3. Complete Phase 3: User Story 1 (Map with station markers)
4. **STOP and VALIDATE**: Map renders, markers appear, location-based centering works
5. Deploy/demo if ready

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready (deps + API client)
2. Add US1 (Map Discovery) → Drivers see stations on map with location
3. Add US2 (Detail Sheet) → Drivers see station details on tap
4. Add US3 (Navigation) → Drivers get directions
5. Add US4 (Radius & Refresh) → Drivers fine-tune discovery
6. Each story adds value without breaking previous stories

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story is independently completable and testable
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break independence
- All API calls go through the API client in src/services/nearby-api.ts
- The nearby endpoint is made public in Phase 1 (T003) — no JWT needed for mobile
- Marker icons use green circle with lightning bolt SVG (consistent with web BaseMap)
- Radius preference persisted via AsyncStorage for future sessions
- Bottom sheet uses `@gorhom/bottom-sheet` with snap points matching content height
