# Implementation Plan: Design System & Components

**Branch**: `003-design-system-components` | **Date**: 2026-06-12 | **Spec**: specs/003-design-system-components/spec.md

**Input**: Feature specification from `specs/003-design-system-components/spec.md`

## Summary

Build the BorneMap shared design system — two pnpm workspace packages under `source/front/packages/`: `@bornemap/tokens` (color, spacing, typography, shadow, and breakpoint design tokens in light/dark modes) and `@bornemap/ui` (Button, Card, Skeleton, EmptyState, ErrorBoundary, ThemeProvider, LoadingOverlay, Badge — all consuming tokens and supporting both React Native/Expo and React/web). The UI/UX Pro Max generated design system at `design-system/bornemap/MASTER.md` serves as the visual reference. Mobile and web driver app UIs are deferred to Phase 4.

## Technical Context

**Language/Version**: TypeScript 5.5+ (strict mode)

**Primary Dependencies**:
- React 19 (web target), React Native 0.76+ (mobile target via Expo SDK 54)
- pnpm (workspace protocol for monorepo)
- CSS Modules or Tailwind for web component styling
- react-native-reanimated v3 for mobile animations (in component code where applicable)
- Storybook 8 or Ladle for component documentation

**Storage**: N/A — design system packages are build artifacts (no runtime persistence)

**Testing**:
- TypeScript strict mode type checking (compilation gate)
- Jest + React Testing Library for unit tests (each component variant)
- Storybook interaction tests for visual states
- Automated WCAG AA contrast checking via tokens CI step
- Visual regression testing (Chromatic or Loki) recommended

**Target Platform**: Cross-platform — Expo/React Native (iOS + Android) and React DOM (web browsers)

**Project Type**: Monorepo design system packages (two npm packages in pnpm workspace)

**Performance Goals**:
- Full library import tree-shaken to <50KB gzipped
- Individual component import pulls zero unused code
- ThemeProvider context change triggers single re-render (no cascading updates)
- Zero runtime dependencies beyond React and tokens

**Constraints**:
- All tokens defined in TypeScript — no hardcoded values in any component
- All components work in both React Native and React DOM (shared implementation where possible, platform files where necessary)
- Dark mode via ThemeProvider, never per-component
- pnpm workspace protocol only — no npm or yarn
- Expo SDK 54 lockstep (no upgrades without ADR)
- WCAG AA minimum contrast for all color pairs
- Semantic versioning for both packages independently
- Source maps in build output

**Scale/Scope**: 2 packages, ~8 UI components, ~150+ design tokens, documentation site with interactive examples

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Justification |
|-----------|--------|---------------|
| I. UX-First | ✅ PASS | Design tokens enforce skeleton-first, dark mode day one, no hardcoded colors. ErrorBoundary and EmptyState fulfill error/empty state requirements. |
| II. Domain-Driven Services | ⏭️ N/A | Frontend-only phase; Domain-Driven Services applies to backend microservices. |
| III. Test-First (NON-NEGOTIABLE) | ✅ PASS | Spec includes explicit type checking, unit tests for every component variant, WCAG AA contrast validation, and visual regression testing. |
| IV. Source-Rooted Codebase | ✅ PASS | All packages under `source/front/packages/`. |
| V. Immutable Data & Append-Only | ⏭️ N/A | No data layer in design system phase. |

**No violations. All gates pass.**

### Re-Check Post Phase 1

| Principle | Status | Justification |
|-----------|--------|---------------|
| I. UX-First | ✅ PASS | Token package enforces skeleton screens, dark mode, and design consistency. UI components include Skeleton, EmptyState, ErrorBoundary, and ThemeProvider. No hardcoded values. |
| II. Domain-Driven Services | ⏭️ N/A | Frontend-only phase. |
| III. Test-First | ✅ PASS | Research confirms RNTL + RTL + Chromatic testing strategy. All 8 components have test coverage requirements. |
| IV. Source-Rooted Codebase | ✅ PASS | All packages under `source/front/packages/`. MASTER.md and data-model under `specs/`. |
| V. Immutable Data & Append-Only | ⏭️ N/A | No data layer. |

**No violations. All gates pass.**

## Project Structure

### Documentation (this feature)

```text
specs/003-design-system-components/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output — token categories, component props
├── quickstart.md        # Phase 1 output — how to build and use the packages
├── contracts/           # Phase 1 output — package API contracts, component prop types
└── tasks.md             # Created by /speckit.tasks
```

### Source Code (repository root)

```text
source/front/packages/
├── tokens/                      # @bornemap/tokens
│   ├── package.json
│   ├── tsconfig.json
│   ├── src/
│   │   ├── index.ts             # Public API — re-exports all token categories
│   │   ├── colors.ts            # Light + dark color palettes
│   │   ├── spacing.ts           # 4px-base spacing scale
│   │   ├── typography.ts        # Font family, sizes, weights, line heights
│   │   ├── shadows.ts           # Elevation tokens
│   │   ├── breakpoints.ts       # Responsive breakpoints
│   │   ├── border-radius.ts     # Shape tokens
│   │   ├── opacity.ts           # Opacity levels
│   │   ├── icon-size.ts         # Icon dimension tokens
│   │   └── types.ts             # TypeScript type definitions
│   └── tsconfig.build.json
│
├── ui/                          # @bornemap/ui
│   ├── package.json
│   ├── tsconfig.json
│   ├── src/
│   │   ├── index.ts             # Public API — re-exports all components
│   │   ├── Button/
│   │   │   ├── Button.tsx
│   │   │   ├── Button.web.tsx   # Platform-specific impl (if needed)
│   │   │   ├── Button.native.tsx
│   │   │   ├── Button.test.tsx
│   │   │   └── Button.stories.tsx
│   │   ├── Card/
│   │   ├── Skeleton/
│   │   ├── EmptyState/
│   │   ├── ErrorBoundary/
│   │   ├── ThemeProvider/
│   │   ├── LoadingOverlay/
│   │   ├── Badge/
│   │   └── shared/              # Shared helpers, utils, platform abstractions
│   │       ├── platform.ts
│   │       └── test-utils.tsx
│   └── tsconfig.build.json
│
└── pnpm-workspace.yaml          # Defines tokens/, ui/ as workspace packages

# Root-level configs (not under packages/)
source/front/
├── package.json                 # Root workspace package.json
├── pnpm-workspace.yaml          # Or at root — determines workspace root
├── tsconfig.base.json           # Shared TypeScript config base
├── .eslintrc.cjs
└── .prettierrc
```

**Structure Decision**: Standard pnpm monorepo with two packages. The `tokens` package is a pure TypeScript data library (no React dependency). The `ui` package depends on `tokens` and React/React Native. Platform-specific component implementations use `.native.tsx` / `.web.tsx` file extensions where React DOM and React Native implementations diverge.

## Complexity Tracking

No violations — all 5 constitution gates pass without justification needed.

## Phase 0: Research

### Unknowns to Resolve

1. **Cross-platform component architecture** — shared React components that render in both React Native and React DOM: pattern for platform-specific files, shared prop interfaces, conditional imports
2. **pnpm workspace setup** — how to configure workspace for Expo SDK 54 + React 19 coexistence, hoisting strategy, dependency resolution
3. **Design token generation** — whether tokens should be hand-written TS, generated from Figma tokens plugin, or synced from the UI/UX Pro Max CSV data
4. **Component testing strategy** — Jest + RTL vs Storybook test runner vs React Native Testing Library for cross-platform component tests
5. **Documentation site** — Storybook 8 with React Native Web vs Ladle vs custom doc site for documenting mobile-first components
6. **Package bundling** — tsup vs tsc vs esbuild for producing ES module output with tree-shaking and `.native`/`.web` extension resolution

### Dependencies

- React 19 and React Native 0.76 compatibility in a pnpm workspace
- Expo SDK 54 module resolution for local packages
- TypeScript `exports` field in package.json for sub-path exports
- Storybook 8 with React Native Web addon for cross-platform preview

### Research Plan

1. **pnpm workspace + Expo SDK 54** — test that a local workspace package can be imported from an Expo app without metro config hacks
2. **Cross-platform React components** — `.native.tsx`/`.web.tsx` file convention vs `platform.ts` abstraction layer
3. **Token strategy** — evaluate UI/UX Pro Max CSV → TypeScript token generation script vs hand-crafted tokens
4. **Component testing** — determine if Jest can run component tests targeting both platforms
5. **Bundle output** — test tsup with `platform: 'neutral'` for universal React components

### Generate Research

Research complete — see `research.md` for consolidated findings. All 6 unknowns resolved.

**Key decisions:**
1. Cross-platform components: `.native.tsx`/`.web.tsx` file convention + `react-native-web` shim
2. pnpm workspace: default isolated mode, SDK 54 `@expo/metro-config` auto-detects pnpm
3. Token generation: lightweight Node script parses `design-system/bornemap/MASTER.md` → `src/*.ts`
4. Component testing: RNTL + RTL in separate files + Chromatic for visual regression
5. Documentation: Storybook 8 with `@storybook/react-native` addon
6. Bundling: tsup with ESM output, `platform: neutral`

### Consolidate findings in `research.md`

