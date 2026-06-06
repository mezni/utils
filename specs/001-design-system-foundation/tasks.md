---
description: "Task list for Design System Foundation sprint (Sprint 1.1)"
---

# Tasks: Design System Foundation

**Input**: Design documents from `/specs/001-design-system-foundation/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/api.md, quickstart.md

**Tests**: Included — FR-011 requires unit tests per variant/state, FR-015 requires all tests pass

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Design System Package**: `packages/ui/src/`
- **Documentation**: `docs/ui/`
- Tests co-located with components in `packages/ui/src/components/[Component]/*.test.tsx`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and package structure

- [X] T001 Create `packages/ui/` directory structure with `src/tokens/`, `src/components/{Button,Input,Badge,StatusBadge,Skeleton,EmptyState,ErrorState,Toast,Modal,Table,StatCard,DataCard}/`, and `src/test/` directories
- [X] T002 Create `packages/ui/package.json` with dependencies: react@^18, react-dom@^18, typescript@^5, vitest@^1, @testing-library/react@^14, @testing-library/jest-dom@^6, @types/react, @types/react-dom, jsdom, eslint, prettier
- [X] T003 Create `packages/ui/tsconfig.json` with strict mode enabled, JSX react-jsx, ES2020 target, paths alias for `@borne-map/ui`
- [X] T004 Create `packages/ui/tsconfig.node.json` for Vite/Node config files
- [X] T005 Create `packages/ui/vite.config.js` with Vitest configuration (environment: jsdom, globals: true, setupFiles: `./src/test/setup.ts`)
- [X] T006 [P] Create `packages/ui/eslint.config.js` with TypeScript rules and `packages/ui/.prettierrc` with project formatting standards

**Checkpoint**: Package infrastructure ready — dependencies installed, build tooling configured

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T007 [P] Create shared TypeScript types in `packages/ui/src/types.ts` (ButtonVariant, ButtonSize, ButtonState, InputVariant, InputSize, InputState, BadgeVariant, StatusBadgeVariant, StatusBadgeState, ToastVariant, ModalSize, SkeletonType, TableColumn, TrendData, DataCardAction)
- [X] T008 [P] Create Vitest test setup in `packages/ui/src/test/setup.ts` importing `@testing-library/jest-dom` matchers and configuring cleanup
- [X] T009 Create `packages/ui/tailwind.config.base.js` extending theme with placeholder token imports (will resolve to actual values once tokens exist)

**Checkpoint**: Foundation ready — types available, test infrastructure ready, Tailwind config scaffolded

---

## Phase 3: User Story 1 — Design Token Package (Priority: P1) 🎯 MVP

**Goal**: Stakeholders can use a single set of design tokens across all applications ensuring visual consistency

**Independent Test**: Load all token files in Node.js and verify exports contain required values with correct types and formats. Tokens must resolve to exact hex/px values when imported.

### Implementation for User Story 1

- [X] T010 [P] [US1] Create color tokens in `packages/ui/src/tokens/colors.ts` — brand (primary, secondary, light, dark), semantic (success, warning, error), neutral scale (100–700), all as `export const` string constants with hex values
- [X] T011 [P] [US1] Create typography tokens in `packages/ui/src/tokens/typography.ts` — font families (sans, mono), font sizes (sm through 4xl) as px numbers, font weights (regular, medium, semibold, bold) as number constants, line heights (tight, normal, relaxed)
- [X] T012 [P] [US1] Create spacing tokens in `packages/ui/src/tokens/spacing.ts` — 4px base unit scale from 0 to 12 units (0, 4, 8, 12, 16, 20, 24, 32, 40, 48, 64) as `export const` number constants
- [X] T013 [P] [US1] Create radius tokens in `packages/ui/src/tokens/radius.ts` — none (0), sm (4), md (8), lg (16), xl (24), full (9999) as `export const` number constants
- [X] T014 [P] [US1] Create shadow tokens in `packages/ui/src/tokens/shadows.ts` — none, card (elevation 2), panel (elevation 4), float (elevation 6), pin (elevation 8) as objects with elevation, shadowColor, shadowOffset, shadowOpacity, shadowRadius, androidElevation
- [X] T015 [US1] Create central token index in `packages/ui/src/tokens/index.ts` re-exporting all tokens from colors, typography, spacing, radius, shadows
- [X] T016 [US1] Create React Native token exports in `packages/ui/src/tokens/native.ts` — re-export colors as strings, spacing as numbers, shadows in React Native StyleSheet-compatible format (elevation, shadowColor, shadowOffset, shadowOpacity, shadowRadius, androidElevation)
- [X] T017 [US1] Create token documentation in `docs/ui/tokens.md` (token categories, import paths, usage examples) and `docs/ui/design-tokens.md` (concrete values table for all tokens)
- [X] T017b [P] [US1] Create token validation utility in `packages/ui/src/tokens/validate.ts` — export `getToken(name, tokensMap)` that throws Error with message "Token [name] is not defined" for undefined values (fulfills spec edge case E1)
- [X] T017c [US1] Update `packages/ui/tailwind.config.base.js` to import real token values — replace placeholder imports with actual imports from `./src/tokens/colors`, `./src/tokens/spacing`, `./src/tokens/radius`, `./src/tokens/typography`, `./src/tokens/shadows`
- [X] T018 [US1] Validate all token exports resolve correctly via test script in Node.js — verify all 5 token categories export required values with correct types

**Checkpoint**: US1 complete — all tokens defined in 5 categories, exported via index.ts + native.ts, documented, build passes

---

## Phase 4: User Story 2 — Web Shared Components (Priority: P1)

**Goal**: Developers can build UI using shared components that automatically inherit design tokens

**Independent Test**: Import each component in a test React application and verify it renders correctly with token-based styles. Each component must work independently with all its variants, sizes, and states.

### Tests and Implementation for User Story 2

**Order**: Tests MUST be written first (they will fail initially), then implementation makes them pass (TDD)

- [X] T019 [P] [US2] Write unit tests for Button in `packages/ui/src/components/Button/Button.test.tsx` — test all 4 variants (primary, secondary, ghost, danger), all 3 sizes (sm, md, lg), all 5 states (default, hover, active, disabled, loading), keyboard accessibility, ARIA labels, focus indicators, RTL support
- [X] T020 [US2] Implement Button component in `packages/ui/src/components/Button/Button.tsx` — token-based colors/spacing/typography/radius, WCAG 2.1 AA accessibility, keyboard navigation, focus ring, ARIA attributes, RTL-aware styles via CSS logical properties
- [X] T021 [P] [US2] Write unit tests for Input in `packages/ui/src/components/Input/Input.test.tsx` — test all 3 variants (default, error, search), all 3 sizes (sm, md, lg), all 4 states (default, focused, error, disabled), error message display, placeholder, onChange callback, accessibility attributes
- [X] T022 [US2] Implement Input component in `packages/ui/src/components/Input/Input.tsx` — token-based borders/colors/spacing/typography/radius, error state styling with token error color, search variant with icon area, disabled state, focus ring, ARIA attributes, RTL support
- [X] T023 [P] [US2] Write unit tests for Badge in `packages/ui/src/components/Badge/Badge.test.tsx` — test all 5 variants (default, success, warning, error, info), children rendering, color contrast validation
- [X] T024 [US2] Implement Badge component in `packages/ui/src/components/Badge/Badge.tsx` — token-based colors for each variant, token spacing/typography/radius, inline display
- [X] T025 [P] [US2] Write unit tests for StatusBadge in `packages/ui/src/components/StatusBadge/StatusBadge.test.tsx` — test all 4 variants (available, in-use, maintenance, offline), showDot toggle, state (default, animating), non-color indicator (dot + text), children rendering
- [X] T026 [US2] Implement StatusBadge component in `packages/ui/src/components/StatusBadge/StatusBadge.tsx` — token-based dot colors per variant, non-color indicator for accessibility, animating state with CSS animation, ARIA live region for status updates
- [X] T027 [P] [US2] Write unit tests for Skeleton in `packages/ui/src/components/Skeleton/Skeleton.test.tsx` — test all 3 types (block, text, circular), width/height props, animated toggle, aria-busy attribute
- [X] T028 [US2] Implement Skeleton component in `packages/ui/src/components/Skeleton/Skeleton.tsx` — token-based background/radius, CSS animation for shimmer effect, type-based shape rendering, aria-busy for accessibility
- [X] T029 [P] [US2] Write unit tests for EmptyState in `packages/ui/src/components/EmptyState/EmptyState.test.tsx` — test icon rendering, required title, optional description, action button rendering and click handler
- [X] T030 [US2] Implement EmptyState component in `packages/ui/src/components/EmptyState/EmptyState.tsx` — token-based spacing/typography/colors, centered layout, action button uses Button component with ghost variant, aria-label for illustration
- [X] T031 [P] [US2] Write unit tests for ErrorState in `packages/ui/src/components/ErrorState/ErrorState.test.tsx` — test icon rendering, required title, optional description, retry button rendering and click handler, error color scheme
- [X] T032 [US2] Implement ErrorState component in `packages/ui/src/components/ErrorState/ErrorState.tsx` — token-based error colors/spacing/typography, retry button uses Button component with danger variant, role="alert" for screen readers
- [X] T033 [P] [US2] Write unit tests for Toast in `packages/ui/src/components/Toast/Toast.test.tsx` — test all 4 variants (success, error, warning, info), title, message, duration, auto-dismiss, close button, onClose callback
- [X] T034 [US2] Implement Toast component in `packages/ui/src/components/Toast/Toast.tsx` — token-based variant colors/spacing/typography/radius/shadow, auto-dismiss with configurable duration, close button, role="alert", slide-in animation, RTL-aware
- [X] T035 [P] [US2] Write unit tests for Modal in `packages/ui/src/components/Modal/Modal.test.tsx` — test all 3 sizes (sm, md, lg), isOpen/onClose, overlay click to close, Escape key to close, focus trap, children rendering, ARIA attributes
- [X] T036 [US2] Implement Modal component in `packages/ui/src/components/Modal/Modal.tsx` — token-based overlay/spacing/radius/shadow/typography, portal rendering, focus trap, Escape/overlay click handlers, role="dialog" + aria-modal, RTL-aware
- [X] T037 [P] [US2] Write unit tests for Table in `packages/ui/src/components/Table/Table.test.tsx` — test column rendering, data rows, sortable columns, row actions, onRowAction callback, empty data handling, accessibility (role="table", aria-sort)
- [X] T038 [US2] Implement Table component in `packages/ui/src/components/Table/Table.tsx` — token-based spacing/typography/colors/radius, sortable column headers with aria-sort, responsive overflow, row actions with icon buttons, semantic table elements
- [X] T039 [P] [US2] Write unit tests for StatCard in `packages/ui/src/components/StatCard/StatCard.test.tsx` — test label, value, trend (positive/negative), icon rendering, accessibility attributes
- [X] T040 [US2] Implement StatCard component in `packages/ui/src/components/StatCard/StatCard.tsx` — token-based spacing/colors/typography/radius/shadow, trend indicator with arrow and token colors (success/error), icon area
- [X] T041 [P] [US2] Write unit tests for DataCard in `packages/ui/src/components/DataCard/DataCard.test.tsx` — test title, action button, children rendering, onClick handler for action
- [X] T042 [US2] Implement DataCard component in `packages/ui/src/components/DataCard/DataCard.tsx` — token-based spacing/colors/typography/radius/shadow, header with title + optional action button, body for children
- [X] T043 [P] [US2] Create components barrel export in `packages/ui/src/components/index.ts` re-exporting all 12 components
- [X] T044 [US2] Create package entry point in `packages/ui/src/index.ts` re-exporting all components from `./components` and tokens from `./tokens`
- [X] T045 [US2] Verify all 12 component test suites exist with variant/state coverage per FR-011 — review each test file includes at least one test per variant/state combination

**Checkpoint**: US2 complete — all 12 components implemented, tested, and exported from package entry

---

## Phase 5: User Story 3 — Component Documentation (Priority: P2)

**Goal**: Developers can find and understand how to use each component via `docs/ui/components.md`

- [X] T046 [P] [US3] Create `docs/ui/components.md` documenting all 12 components with: description, TypeScript props table, usage examples for each variant/state combination, accessibility notes, RTL behavior notes
- [X] T047 [US3] Verify `docs/ui/components.md` contains entries for all 12 components with required sections (description, props, examples, accessibility)

**Checkpoint**: US3 complete — all components documented, searchable, with copy-pasteable examples

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Final verification, build hardening, and README

- [X] T048 [P] Run `pnpm build` — verify zero warnings across token package and component build
- [X] T049 [P] Run `pnpm test` — verify all component tests pass with full variant/state coverage (FR-014, FR-015)
- [X] T050 Validate `packages/ui/tailwind.config.base.js` resolves all token values without errors (FR-016)
- [X] T050b [P] Add token resolution benchmark in `packages/ui/src/tokens/benchmark.test.ts` — verify all tokens resolve in <10ms using `performance.now()`
- [X] T051 Create `packages/ui/README.md` with package overview, installation instructions, quick usage examples, and links to docs/ui/

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **US1 (Phase 3)**: Depends on Foundational completion — no dependencies on other stories
- **US2 (Phase 4)**: Depends on Foundational completion — needs US1 tokens to be available for visual values
- **US3 (Phase 5)**: Depends on US2 completion — needs all components implemented before documenting
- **Polish (Phase 6)**: Depends on all user stories being complete

### User Story Dependencies

- **US1 — Design Token Package (P1)**: Can start after Foundational — NO dependencies on other stories ★ MVP
- **US2 — Web Shared Components (P1)**: Requires US1 tokens — components consume token values for all visuals
- **US3 — Component Documentation (P2)**: Requires US2 — documents components that must exist first

### Within Each User Story

- Tests MUST be written and FAIL before implementation (within each component pair)
- Token files within US1 can all run in parallel (T010–T014)
- Components within US2 can all run in parallel (all 12 component pairs are independent of each other)

### Parallel Opportunities

| Phase | Parallel Tasks |
|-------|---------------|
| Phase 1 | T006 (ESLint + Prettier configs) |
| Phase 2 | T007 (types.ts) + T008 (test setup) |
| Phase 3 | T010–T014 (all 5 token files) + T017b (validation utility) — all independent |
| Phase 4 | T019+T021+T023+T025+T027+T029+T031+T033+T035+T037+T039+T041 (all 12 test files) can run in parallel |
| Phase 4 | T020+T022+T024+T026+T028+T030+T032+T034+T036+T038+T040+T042 (all 12 component files) can run in parallel |
| Phase 5 | T046 (single doc file) |
| Phase 6 | T048 (build) + T049 (test) + T050 (tailwind validate) + T050b (benchmark) + T051 (README) — all independent |

---

## Parallel Example: User Story 2

```bash
# Launch all 12 test files in parallel (they only depend on tokens + test setup):
pnpm vitest run packages/ui/src/components/*/*.test.tsx &

# Once all tests are written and failing, implement all 12 components in parallel:
# (Each component is in its own directory with no cross-dependencies)

# After all components are implemented and tests pass:
pnpm vitest run
```

---

## Implementation Strategy

### MVP First (US1 Only)

1. Complete Phase 1: Setup (T001–T006)
2. Complete Phase 2: Foundational (T007–T009)
3. Complete Phase 3: US1 Design Token Package (T010–T018)
4. **STOP and VALIDATE**: `pnpm build` passes, all tokens export correctly
5. Deploy/demo token package — immediate value to all applications

### Incremental Delivery

1. **Setup + Foundational** → Foundation ready for all development
2. **US1 (Tokens)** → Token package available → Deploy/Demo (MVP!)
3. **US2 (Components)** → All 12 components available → Deploy/Demo
4. **US3 (Docs)** → Documentation complete → Sprint complete
5. **Polish** → Final verification, build passes, README done

### Parallel Team Strategy

With multiple developers:

1. Team completes Phase 1 + Phase 2 together
2. Once Foundation is done:
   - Developer A: Phase 3 (US1 — all 5 token files + index + native + docs)
   - Developer B: Phase 4 components group 1 (Button, Input, Badge, StatusBadge, Skeleton, EmptyState)
   - Developer C: Phase 4 components group 2 (ErrorState, Toast, Modal, Table, StatCard, DataCard)
3. Phase 5 (US3 docs) can start once components are implemented
4. Phase 6 polish after everything is done

---

## Task Summary

| Phase | Task Count | Story | Priority |
|-------|-----------|-------|----------|
| Phase 1: Setup | 6 | — | — |
| Phase 2: Foundational | 3 | — | — |
| Phase 3: US1 — Tokens | 11 | [US1] | P1 ★ MVP |
| Phase 4: US2 — Components | 27 | [US2] | P1 |
| Phase 5: US3 — Docs | 2 | [US3] | P2 |
| Phase 6: Polish | 5 | — | — |
| **Total** | **54** | | |

### Tests Included

- 12 component test suites (one per component) — T019, T021, T023, T025, T027, T029, T031, T033, T035, T037, T039, T041
- Each test suite covers all variants, sizes, states per FR-011
- Tests written before implementation (TDD)

### Format Validation

☑ All tasks use `- [ ] [TaskID] [optional P] [optional Story] Description with file path`
☑ Task IDs are sequential (T001–T051)
☑ Parallel tasks marked with `[P]`
☑ User story tasks marked with `[US1]`, `[US2]`, `[US3]`
☑ Exact file paths included in every task description
☑ Each user story has independent test criteria defined
