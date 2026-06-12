# Tasks: Design System & Components

**Input**: Design documents from `specs/003-design-system-components/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

## Format: `[ID] [P] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1)
- Include exact file paths in descriptions

## Path Conventions

- Workspace root: `source/front/`
- Token package: `source/front/packages/tokens/`
- UI package: `source/front/packages/ui/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Initialize pnpm monorepo workspace, TypeScript configuration, and shared tooling

- [ ] T001 Create root `package.json` at `source/front/package.json` with pnpm workspace config and shared scripts
- [ ] T002 Create `source/front/pnpm-workspace.yaml` listing `packages/*`, `mobile-driver`, `web-driver`, `dashboard`
- [ ] T003 Create `source/front/tsconfig.base.json` with strict mode, ESNext target, and path aliases
- [ ] T004 [P] Create `.eslintrc.cjs` at `source/front/.eslintrc.cjs` with TypeScript + React rules
- [ ] T005 [P] Create `.prettierrc` at `source/front/.prettierrc` with project formatting standards
- [ ] T006 Create `source/front/.npmrc` (empty or minimal — pnpm isolated mode default)

---

## Phase 2: Foundational (Blocking Prerequisites)

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T007 Create `source/front/packages/tokens/package.json` with `@bornemap/tokens` name, `tsup` build script, and ESM export config
- [ ] T008 Create `source/front/packages/tokens/tsconfig.json` extending `../../tsconfig.base.json` with build output settings
- [ ] T009 [P] Create `source/front/packages/ui/package.json` with `@bornemap/ui` name, `tsup` build script, and peer deps on `react` + `react-native`
- [ ] T010 Create `source/front/packages/ui/tsconfig.json` extending `../../tsconfig.base.json`
- [ ] T011 [P] Configure tsup in both packages for ESM output, `platform: neutral`, `.d.ts` generation, and source maps
- [ ] T012 Setup Jest test infrastructure: root `jest.config.ts`, shared test utils, and `@testing-library/react-native` + `@testing-library/react` presets
- [ ] T013 Setup Storybook 8 with `@storybook/react` and `@storybook/react-native` addons for component documentation
- [ ] T014 Setup Chromatic project configuration for visual regression testing
- [ ] T015 Add `typecheck`, `lint`, and `test` workspace scripts to root `package.json`

**Checkpoint**: Foundation ready — workspace builds, TypeScript compiles, test runner works, Storybook launches

---

## Phase 3: User Story 1 - Shared Design System & UI Kit (Priority: P1) 🎯 MVP

**Goal**: Build `@bornemap/tokens` with all design token categories and `@bornemap/ui` with 8 core components consuming those tokens, all cross-platform (React Native + React DOM).

**Independent Test**: Build both packages, run typecheck, verify components render in a minimal Expo app and a React web app.

### Token Package Implementation

- [ ] T016 [US1] Create `source/front/packages/tokens/src/index.ts` as the public API re-exporting all token categories
- [ ] T017 [P] [US1] Create `source/front/packages/tokens/src/types.ts` with TypeScript type definitions for all token categories (`ColorScheme`, `SpacingKey`, `TypographyTokens`, etc.)
- [ ] T018 [P] [US1] Create `source/front/packages/tokens/src/colors.ts` exporting light and dark color palettes (20 color roles each) based on `design-system/bornemap/MASTER.md`
- [ ] T019 [P] [US1] Create `source/front/packages/tokens/src/spacing.ts` exporting 4px-base spacing scale (0, 4, 8, 12, 16, 20, 24, 32, 40, 48, 64)
- [ ] T020 [P] [US1] Create `source/front/packages/tokens/src/typography.ts` exporting font family (Inter), size scale, weight scale, and line-height tokens
- [ ] T021 [P] [US1] Create `source/front/packages/tokens/src/shadows.ts` exporting elevation tokens (sm, md, lg, xl)
- [ ] T022 [P] [US1] Create `source/front/packages/tokens/src/breakpoints.ts` exporting responsive breakpoint tokens
- [ ] T023 [P] [US1] Create `source/front/packages/tokens/src/border-radius.ts` exporting radius tokens (none, sm, md, lg, full)
- [ ] T024 [P] [US1] Create `source/front/packages/tokens/src/opacity.ts` exporting opacity tokens
- [ ] T025 [P] [US1] Create `source/front/packages/tokens/src/icon-size.ts` exporting icon dimension tokens
- [ ] T025b [P] [US1] Create `source/front/packages/tokens/src/css.ts` generating CSS custom property definitions (`--color-primary`, `--spacing-4`, etc.) for web consumption via `react-native-web` style injection
- [ ] T026 [US1] Write token generation script at `source/front/packages/tokens/scripts/generate-from-mastermd.ts` that parses `design-system/bornemap/MASTER.md` tables and regenerates `src/*.ts` files
- [ ] T027 [US1] Build and verify `@bornemap/tokens` package compiles with zero type errors and all exports are accessible

### UI Component Package Implementation

- [ ] T028 [US1] Create `source/front/packages/ui/src/index.ts` as the public API re-exporting all components
- [ ] T029 [P] [US1] Create `ThemeProvider` component at `source/front/packages/ui/src/ThemeProvider/ThemeProvider.tsx` with light/dark/system mode context and `useTheme` hook
- [ ] T030 [P] [US1] Create `Button` component at `source/front/packages/ui/src/Button/Button.tsx` supporting 5 variants, 3 sizes, loading, disabled, and full-width states — consuming `@bornemap/tokens`
- [ ] T031 [P] [US1] Create `Button.web.tsx` and `Button.native.tsx` platform variants if needed for pressable behavior differences
- [ ] T032 [P] [US1] Create `Card` component at `source/front/packages/ui/src/Card/Card.tsx` with 3 variants (default, elevated, interactive), header/content/footer slots
- [ ] T033 [P] [US1] Create `Skeleton` component at `source/front/packages/ui/src/Skeleton/Skeleton.tsx` with rectangular, circular, and text line shapes + animated pulse via `react-native-reanimated` (web: CSS animation fallback)
- [ ] T034 [P] [US1] Create `EmptyState` component at `source/front/packages/ui/src/EmptyState/EmptyState.tsx` with icon slot, title, description, and action button
- [ ] T035 [P] [US1] Create `ErrorBoundary` component at `source/front/packages/ui/src/ErrorBoundary/ErrorBoundary.tsx` with fallback UI and retry action
- [ ] T036 [P] [US1] Create `LoadingOverlay` component at `source/front/packages/ui/src/LoadingOverlay/LoadingOverlay.tsx` with configurable message and cancel action
- [ ] T037 [P] [US1] Create `Badge` component at `source/front/packages/ui/src/Badge/Badge.tsx` with 5 variants and 3 sizes
- [ ] T037b [P] [US1] Create `Button.test.tsx`, `Card.test.tsx`, `Skeleton.test.tsx`, `EmptyState.test.tsx`, `ErrorBoundary.test.tsx`, `ThemeProvider.test.tsx`, `LoadingOverlay.test.tsx`, `Badge.test.tsx` — unit tests covering all variants, states, and edge cases
- [ ] T038 [P] [US1] Create `source/front/packages/ui/src/shared/platform.ts` with platform abstraction helpers for cross-platform compatibility
- [ ] T039 [US1] Build and verify `@bornemap/ui` package compiles with zero type errors, all components render in both environments
- [ ] T040 [US1] Write Storybook stories for all 8 components covering every variant and state at `source/front/packages/ui/src/**/*.stories.tsx`
- [ ] T041 [US1] Add WCAG AA contrast validation script at `source/front/packages/ui/scripts/validate-contrast.ts` checking all color foreground/background pairs
- [ ] T041b [US1] Add bundle-size analysis step to tsup config and CI — measure gzipped size of full `@bornemap/ui` import, fail if >50KB
- [ ] T041c [US1] Add ThemeProvider render-cycle test — verify mode change completes in a single render cycle with no visual flash

**Checkpoint**: Design system complete — both packages build, all 8 components render cross-platform, Storybook documented, contrast validated

---

## Phase 4: Polish & Cross-Cutting Concerns

**Purpose**: Final validation, documentation, and CI integration

- [ ] T042 [P] Add CI workflow at `.github/workflows/design-system.yml` building both packages, running typecheck, lint, and tests
- [ ] T043 [P] Configure Chromatic CI integration for automatic visual regression on PRs
- [ ] T044 [P] Write package README files at `source/front/packages/tokens/README.md` and `source/front/packages/ui/README.md`
- [ ] T045 Run quickstart.md validation — verify a consumer can install and use both packages
- [ ] T046 Update `AGENTS.md` to reference this completed phase

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS user story
- **User Story 1 (Phase 3)**: Depends on Foundational completion — must build tokens first (`@bornemap/tokens` before `@bornemap/ui`)
- **Polish (Phase 4)**: Depends on User Story 1 completion

### Within User Story 1

1. Build `@bornemap/tokens` (T016-T027) — zero runtime deps, can be done first
2. Build `@bornemap/ui` (T028-T041) — depends on tokens package
3. All token category files (T017-T025) can run in parallel
4. All UI components (T029-T037) can run in parallel once ThemeProvider exists
5. Validate and document at the end

### Parallel Opportunities

- T004–T005 (linting/formatting configs) can run in parallel
- T007–T010 (package scaffolding) can run in parallel
- All token source files (T017-T025) can run in parallel
- All UI components (T029-T037) can run in parallel
- T042–T044 (CI, Chromatic, READMEs) can run in parallel

---

## Parallel Example: User Story 1

```bash
# Launch all token files together:
Task: "Create types.ts in source/front/packages/tokens/src/"
Task: "Create colors.ts in source/front/packages/tokens/src/"
Task: "Create spacing.ts in source/front/packages/tokens/src/"
Task: "Create typography.ts in source/front/packages/tokens/src/"
Task: "Create shadows.ts in source/front/packages/tokens/src/"

# Launch all UI components together (after ThemeProvider):
Task: "Create Button in source/front/packages/ui/src/Button/"
Task: "Create Card in source/front/packages/ui/src/Card/"
Task: "Create Skeleton in source/front/packages/ui/src/Skeleton/"
Task: "Create EmptyState in source/front/packages/ui/src/EmptyState/"
Task: "Create ErrorBoundary in source/front/packages/ui/src/ErrorBoundary/"
Task: "Create LoadingOverlay in source/front/packages/ui/src/LoadingOverlay/"
Task: "Create Badge in source/front/packages/ui/src/Badge/"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL — blocks everything)
3. Complete Phase 3: User Story 1 → `@bornemap/tokens` first, then `@bornemap/ui`
4. **STOP and VALIDATE**: Test the design system independently
5. Phase 4: Polish

### Incremental Delivery

1. Complete Setup + Foundational → Workspace ready
2. Add `@bornemap/tokens` → Build and verify (token-only MVP!)
3. Add `@bornemap/ui` components one by one → Each independently verifiable
4. Add Storybook + CI → Complete design system

---

## Notes

- [P] tasks = different files, no dependencies
- [US1] labels map to the single user story
- Token package has zero runtime dependencies — can be built and tested in isolation
- UI package depends on tokens but each component is independently importable (tree-shakeable)
- Phase 4 driver apps are deferred — do not build app scaffolding in this phase
