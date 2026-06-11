# Tasks: Design System — UI Primitives & Tokens

**Input**: Design documents from `/specs/005-design-system/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/components.md, quickstart.md

**Tests**: Tests are included per the spec's Independent Test requirements and research.md test coverage definitions.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Package root**: `source/front/packages/design-system/`
- **Tokens**: `src/tokens/`
- **Components**: `src/components/<ComponentName>/`
- **Tests**: `tests/components/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Initialize the design-system package — directory structure, config files, and dependencies

- [ ] T001 Create design-system package directory structure at `source/front/packages/design-system/`
- [ ] T002 [P] Initialize package.json with name `@borne/design-system`, scripts (test, storybook), and dependencies (react-native, react-native-reanimated, expo-haptics) in `source/front/packages/design-system/package.json`
- [ ] T003 [P] Create tsconfig.json with React Native + Reanimated configuration at `source/front/packages/design-system/tsconfig.json`
- [ ] T004 [P] Create Jest config with React Native Testing Library preset at `source/front/packages/design-system/jest.config.js`
- [ ] T005 Install all project dependencies for design-system package

---

## Phase 2: User Story 1 — Design Tokens (Priority: P1) 🎯 MVP

**Goal**: A centralized design tokens module exporting typed color (light/dark), spacing, typography, radii, and shadow values — plus ThemeProvider for automatic dark mode switching via Appearance API.

**Independent Test**: Import tokens into a test harness, apply each token category to a rendered view, verify values match the expected hex/px values. Toggle device to dark mode — all color tokens switch automatically within one frame.

### Implementation for User Story 1

- [ ] T006 [P] [US1] Create light and dark color palettes (primary, secondary, background, surface, text, error, success, border, skeleton) in `src/tokens/colors.ts`
- [ ] T007 [P] [US1] Create spacing scale (4px base: 4, 8, 12, 16, 20, 24, 32, 48, 64) in `src/tokens/spacing.ts`
- [ ] T008 [P] [US1] Create typography scale (fontFamily, fontSize, fontWeight, lineHeight) in `src/tokens/typography.ts`
- [ ] T009 [P] [US1] Create radii scale (none, sm, md, lg, full) in `src/tokens/radii.ts`
- [ ] T010 [P] [US1] Create shadow presets (sm, md, lg, xl) in `src/tokens/shadows.ts`
- [ ] T011 [US1] Create ThemeContext with ThemeProvider (reads useColorScheme + Appearance API) and useTheme hook in `src/tokens/ThemeContext.tsx`
- [ ] T012 [US1] Create tokens barrel export in `src/tokens/index.ts`

**Checkpoint**: Design tokens are importable, typed, and theme-aware. Dark mode switching works via system toggle.

---

## Phase 3: User Story 2 — Button Component (Priority: P1)

**Goal**: A reusable Button primitive supporting primary/secondary/ghost variants, disabled/loading states, press scale animation, and haptic feedback via expo-haptics.

**Independent Test**: Render button with each variant, verify label renders. Tap fires onPress callback. Disabled button does not fire onPress. Loading button shows ActivityIndicator and ignores taps. Haptic fires on physical device.

### Tests for User Story 2

- [ ] T013 [US2] Write Button tests covering all variants, disabled state, loading state, and onPress callback in `tests/components/Button.test.tsx`

### Implementation for User Story 2

- [ ] T014 [US2] Implement Button component with Reanimated v3 scale animation and expo-haptics in `src/components/Button/Button.tsx`
- [ ] T015 [P] [US2] Create Button Storybook stories for each variant/state in `src/components/Button/Button.stories.tsx`
- [ ] T016 [P] [US2] Create Button barrel export in `src/components/Button/index.ts`

**Checkpoint**: Button renders all variants, responds to taps with haptic + animation, and rejects taps when disabled/loading.

---

## Phase 4: User Story 3 — Skeleton Loader (Priority: P1)

**Goal**: Animated skeleton placeholders for map (full-screen rectangle) and list (rows with avatar + text) layouts with shimmer animation running on UI thread via Reanimated v3.

**Independent Test**: Render map skeleton — full-screen shimmering rectangle visible. Render list skeleton with rows=5 — 5 shimmer rows rendered. Verify animation is smooth (60fps) and loops continuously.

### Tests for User Story 3

- [ ] T017 [US3] Write Skeleton tests covering map layout, list layout with custom rows, and shimmer animation presence in `tests/components/Skeleton.test.tsx`

### Implementation for User Story 3

- [ ] T018 [US3] Implement Skeleton component with Reanimated v3 shimmer animation and map/list variants in `src/components/Skeleton/Skeleton.tsx`
- [ ] T019 [P] [US3] Create Skeleton Storybook stories for map and list layouts in `src/components/Skeleton/Skeleton.stories.tsx`
- [ ] T020 [P] [US3] Create Skeleton barrel export in `src/components/Skeleton/index.ts`

**Checkpoint**: Skeleton renders and animates for both map and list layouts. Shimmer loops without jank.

---

## Phase 5: User Story 4 — Empty State (Priority: P2)

**Goal**: A composable EmptyState component with title, description, optional illustration, and optional CTA button. Used for "no stations nearby" and "GPS unavailable" scenarios.

**Independent Test**: Render empty state with title only — title appears centered. Render with title, description, and CTA — all elements visible. Tap CTA fires onCtaPress callback.

### Tests for User Story 4

- [ ] T021 [US4] Write EmptyState tests covering title rendering, description, CTA button, and onCtaPress callback in `tests/components/EmptyState.test.tsx`

### Implementation for User Story 4

- [ ] T022 [US4] Implement EmptyState component with optional illustration slot and Button-based CTA in `src/components/EmptyState/EmptyState.tsx`
- [ ] T023 [P] [US4] Create EmptyState Storybook stories with/without CTA in `src/components/EmptyState/EmptyState.stories.tsx`
- [ ] T024 [P] [US4] Create EmptyState barrel export in `src/components/EmptyState/index.ts`

**Checkpoint**: EmptyState renders all elements correctly. CTA invokes callback. Composes with parent loading/error logic.

---

## Phase 6: User Story 5 — Error State (Priority: P2)

**Goal**: An ErrorState component with error icon, descriptive message, and retry button. Integrates with Button primitive for the retry CTA.

**Independent Test**: Render error state with message — message displayed. Retry button visible. Tap retry fires onRetry callback.

### Tests for User Story 5

- [ ] T025 [US5] Write ErrorState tests covering message rendering, retry button visibility, and onRetry callback in `tests/components/ErrorState.test.tsx`

### Implementation for User Story 5

- [ ] T026 [US5] Implement ErrorState component with error icon, message, and Button-based retry CTA in `src/components/ErrorState/ErrorState.tsx`
- [ ] T027 [P] [US5] Create ErrorState Storybook stories with sample error messages in `src/components/ErrorState/ErrorState.stories.tsx`
- [ ] T028 [P] [US5] Create ErrorState barrel export in `src/components/ErrorState/index.ts`

**Checkpoint**: ErrorState renders message + retry button. Tap retry calls onRetry. Works alongside loading/empty state flow.

---

## Phase 7: User Story 6 — Bottom Sheet (Priority: P2)

**Goal**: A Reanimated v3 bottom sheet with spring open/close animation, gesture-driven swipe-to-dismiss, configurable snap points, and scrollable content support.

**Independent Test**: Render bottom sheet with isOpen=true — sheet animates up with spring. Swipe down past threshold — sheet dismisses, onClose called. Scrollable content scrolls independently of sheet gesture.

### Tests for User Story 7

- [ ] T029 [US6] Write BottomSheet tests covering children rendering, open/close animation state, and gesture dismiss in `tests/components/BottomSheet.test.tsx`

### Implementation for User Story 7

- [ ] T030 [US6] Implement BottomSheet component with Reanimated v3 spring animation, Pan gesture, configurable snap points, and scrollable content container in `src/components/BottomSheet/BottomSheet.tsx`
- [ ] T031 [P] [US6] Create BottomSheet Storybook stories with sample content and configurable snap points in `src/components/BottomSheet/BottomSheet.stories.tsx`
- [ ] T032 [P] [US6] Create BottomSheet barrel export in `src/components/BottomSheet/index.ts`

**Checkpoint**: BottomSheet animates open/closed with Reanimated v3 spring. Swipe-to-dismiss works. Scrollable content does not conflict with drag gesture.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Final integration, barrel exports, and verification

- [ ] T033 Create root package barrel export in `src/index.ts` (re-exports all components and tokens)
- [ ] T034 Run quickstart.md verification checklist — all 7 checks pass

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **US1 Design Tokens (Phase 2)**: Depends on Setup — BLOCKS all subsequent phases
- **US2 Button (Phase 3)**: Depends on US1 (uses color, spacing, typography tokens)
- **US3 Skeleton (Phase 4)**: Depends on US1 (uses color tokens for skeleton/shimmer)
- **US4 EmptyState (Phase 5)**: Depends on US1 (tokens) and US2 (Button for CTA)
- **US5 ErrorState (Phase 6)**: Depends on US1 (tokens) and US2 (Button for retry)
- **US6 BottomSheet (Phase 7)**: Depends on US1 (tokens)
- **Polish (Phase 8)**: Depends on all phases complete

### User Story Dependency Graph

```
US1 (Tokens) ───┬── US2 (Button) ───┬── US4 (EmptyState)
                 │                   └── US5 (ErrorState)
                 ├── US3 (Skeleton)
                 └── US6 (BottomSheet)
```

### Within Each User Story

- Tests written first (expect failure), then implementation
- Implementation before Storybook stories
- Index barrel export last
- Story complete before moving to next phase

### Parallel Opportunities

- All token files (T006–T010) can be created in parallel
- US2 (Button) and US3 (Skeleton) and US6 (BottomSheet) can be implemented in parallel after US1 completes
- US4 (EmptyState) and US5 (ErrorState) can be implemented in parallel after US2 completes
- Within each component phase: story + barrel can run in parallel with implementation cleanup tasks

---

## Parallel Example: Phase 2 (Tokens)

```bash
# Launch all token file creation in parallel:
Task: "Create color tokens in src/tokens/colors.ts"
Task: "Create spacing scale in src/tokens/spacing.ts"
Task: "Create typography scale in src/tokens/typography.ts"
Task: "Create radii scale in src/tokens/radii.ts"
Task: "Create shadow presets in src/tokens/shadows.ts"
```

## Parallel Example: Phases 3, 4, 7 (P1 + P2 parallelism)

```bash
# After US1 completes, launch Button, Skeleton, and BottomSheet in parallel:
Task: "Implement Button in src/components/Button/Button.tsx"
Task: "Implement Skeleton in src/components/Skeleton/Skeleton.tsx"
Task: "Implement BottomSheet in src/components/BottomSheet/BottomSheet.tsx"
```

---

## Implementation Strategy

### MVP First (US1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: US1 Design Tokens
3. **STOP and VALIDATE**: Import tokens in a test harness, verify values, toggle dark mode
4. Foundation is ready for all future UI work

### Incremental Delivery

1. Setup + US1 Tokens → Foundation ready
2. Add US2 Button → Test independently → Deploy/Demo
3. Add US3 Skeleton → Test independently → Can be parallel with US2
4. Add US6 BottomSheet → Test independently → Can be parallel with US2/US3
5. Add US4 EmptyState + US5 ErrorState → Test independently → After US2 (Button)

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + US1 (Tokens) together
2. Developer A: US2 Button + US4 EmptyState + US5 ErrorState
3. Developer B: US3 Skeleton
4. Developer C: US6 BottomSheet
5. Stories complete and integrate independently

---

## Notes

- [P] tasks = different files, no dependencies — can run in parallel
- [Story] label maps task to specific user story for traceability
- Each user story is independently testable per the spec's Independent Test section
- Write tests first (expect failure), then implement component
- All style values must reference design tokens — zero hardcoded values
- Reanimated v3 only — no React Native Animated API
- Commit after each story phase or logical group
