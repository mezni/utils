# Feature Specification: Design System & Components (MVP-1 Phase 3)

**Feature Branch**: `003-design-system-components`

**Created**: 2026-06-12

**Status**: Draft

**Input**: User description: "read from mvp 1 phase 3"

## Clarifications

### Session 2026-06-12

- Q: How should text search by address be implemented when the backend only supports spatial (lat/lng/radius) queries? → A: Use client-side geocoding via a free OSM Nominatim API call in the app to convert address text to coordinates, then pass those to the existing `/nearby` endpoint. No backend changes needed.
- Q: Scope — should Phase 3 deliver mobile driver app only, or include the web driver as well? → A: Both simultaneously — full mobile driver app (Expo) and web driver (React + Leaflet) in Phase 3.
- Q: What is the default radius for initial station fetch when the map loads? → A: 10 km default radius with dynamic auto-expansion if fewer than 5 stations found.
- Q: How should the shared design system be built given `source/front/packages/` is empty? → A: Build foundational packages in Phase 3: `@bornemap/tokens` (colors, spacing, typography) and `@bornemap/ui` (Button, Card, Skeleton, EmptyState, ErrorBoundary) as pnpm workspace packages. Both apps consume them.
- Q: What is the scope of Phase 3 given the feature is now "Design System & Components"? → A: Phase 3 is design system only — build `@bornemap/tokens` and `@bornemap/ui` packages. Mobile and web driver apps are deferred to Phase 4.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Shared Design System & UI Kit (Priority: P1)

As a developer, I want a shared design system package with tokens and core UI components so that both the mobile and web driver apps have consistent branding, layout, and behavior without duplicating code.

**Why this priority**: The design system is the foundation for all frontend work. Without it, each app must reimplement tokens and components, violating Constitution IV (Source-Rooted Codebase) and increasing maintenance cost.

**Independent Test**: Build `@bornemap/tokens` and confirm it exports color, spacing, typography, and shadow tokens. Build `@bornemap/ui` and confirm it exports at minimum: Button, Card, Skeleton, EmptyState, and ErrorBoundary — all consuming tokens and rendering correctly in both Expo (mobile) and React (web) environments.

**Acceptance Scenarios**:

1. **Given** the monorepo workspace is configured with pnpm, **When** `@bornemap/tokens` is built under `source/front/packages/tokens`, **Then** it exports all design tokens (colors, spacing, typography, shadows, breakpoints) as structured TypeScript objects with full TypeScript type definitions
2. **Given** `@bornemap/ui` exists under `source/front/packages/ui`, **When** it imports tokens and exports core components, **Then** all components use tokens exclusively — no hardcoded colors, spacing, or typography values
3. **Given** the tokens package provides light and dark color schemes, **When** an app toggles between themes, **Then** all token-based components update automatically without per-component dark mode logic
4. **Given** a component is rendered, **When** inspected, **Then** it passes WCAG AA contrast requirements (4.5:1 for text, 3:1 for large text) in both light and dark modes
5. **Given** both platform environments, **When** components are rendered in Expo (React Native) and React (web), **Then** they produce visually identical output from the same token and component code

### User Stories 2-4 — Deferred to Phase 4

The following user stories are **out of scope for Phase 3** and will be implemented in Phase 4:

1. **Station Discovery Map** (Explore stations on map, search/filter, view details & navigate)
2. **Mobile Driver App** (Expo SDK 54)
3. **Web Driver App** (React 19 + Leaflet)

Phase 3 delivers only the design system foundation (`@bornemap/tokens` + `@bornemap/ui`) that these apps will consume.

### Edge Cases

- What happens when a platform (iOS vs Android vs web) renders a component differently? (platform-specific overrides in component code, documented in stories)
- How are token changes communicated to consumers? (semantic versioning in package.json, changelog per release)
- What happens when a consumer needs a token value that doesn't exist? (extend tokens via TypeScript module augmentation, document in contribution guide)

## Requirements *(mandatory)*

### Functional Requirements

#### Token Package (`@bornemap/tokens`)

- **FR-001**: `@bornemap/tokens` MUST export color tokens including primary, secondary, accent, background, foreground, muted, border, destructive, success, warning, and info — each with light and dark variants
- **FR-002**: `@bornemap/tokens` MUST export spacing tokens using a 4px base unit scale (4, 8, 12, 16, 20, 24, 32, 40, 48, 64)
- **FR-003**: `@bornemap/tokens` MUST export typography tokens including font family (Inter), font size scale, font weight scale, line height scale, and letter-spacing values
- **FR-004**: `@bornemap/tokens` MUST export shadow/elevation tokens for card, dropdown, modal, and toast levels
- **FR-005**: `@bornemap/tokens` MUST export breakpoint tokens (mobile, tablet, desktop, wide) as number values
- **FR-006**: `@bornemap/tokens` MUST export border radius tokens (none, sm, md, lg, full)
- **FR-007**: `@bornemap/tokens` MUST export opacity tokens for disabled, overlay, and subtle states
- **FR-008**: `@bornemap/tokens` MUST export icon size tokens (sm, md, lg, xl)
- **FR-009**: All tokens MUST have TypeScript type definitions exported alongside their values
- **FR-010**: All tokens MUST be expressed as CSS custom properties for web and as JavaScript objects for React Native
- **FR-011**: Color tokens MUST pass WCAG AA contrast ratio between foreground and background pairs in both light and dark modes (4.5:1 for text, 3:1 for large text)
- **FR-012**: Token values MUST be the single source of truth — no hardcoded values in any consuming package

#### UI Component Package (`@bornemap/ui`)

- **FR-013**: `@bornemap/ui` MUST export a `Button` component supporting variants (primary, secondary, outline, ghost, destructive), sizes (sm, md, lg), loading state, disabled state, and full-width mode
- **FR-014**: `@bornemap/ui` MUST export a `Card` component supporting variants (default, elevated, interactive), header/content/footer slots, and pressable/hover states
- **FR-015**: `@bornemap/ui` MUST export a `Skeleton` component supporting rectangular, circular, and text line shapes with animated pulse effect
- **FR-016**: `@bornemap/ui` MUST export an `EmptyState` component with configurable icon, title, description, and action slot for recovery CTAs
- **FR-017**: `@bornemap/ui` MUST export an `ErrorBoundary` component that catches rendering errors and displays a fallback UI with retry action
- **FR-018**: `@bornemap/ui` MUST export a `ThemeProvider` component that accepts a mode (light/dark/system) and provides theme context to all children
- **FR-019**: `@bornemap/ui` MUST export a `LoadingOverlay` component with configurable message and optional cancel action
- **FR-020**: `@bornemap/ui` MUST export a `Badge` component supporting variants (default, success, warning, error, info) and sizes
- **FR-021**: All components MUST accept a `className` prop for web and `style` prop for React Native for consumer-level overrides
- **FR-022**: All components MUST support dark mode via the `ThemeProvider` context — no per-component dark mode logic
- **FR-023**: All components MUST be tree-shakeable — importing one component MUST NOT pull in unused components

#### Workspace & Build

- **FR-024**: Package manager MUST be pnpm with workspace protocol
- **FR-025**: Mobile app MUST use Expo SDK 54 lockstep version
- **FR-026**: Web app MUST use React 19 with Leaflet for the map component
- **FR-027**: Both packages MUST build with TypeScript strict mode enabled
- **FR-028**: Both packages MUST produce ES module output for tree-shaking
- **FR-029**: Both packages MUST include source maps in the build output
- **FR-030**: A shared Storybook 8 instance MUST be configured to preview all components in both web and native modes

#### Design Reference

- **FR-031**: The UI/UX Pro Max generated design system at `design-system/bornemap/MASTER.md` MUST be used as the visual reference for token values and component styling

### Key Entities *(include if feature involves data)*

- **Design Token**: A named value representing a design decision (color, spacing, typography). Organized by category and name. Exported from `@bornemap/tokens` as typed constants.
- **UI Component**: A reusable visual element built from design tokens. Exported from `@bornemap/ui` as platform-agnostic React components (works in both React Native and React DOM).

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: `@bornemap/tokens` package builds with zero type errors
- **SC-002**: `@bornemap/ui` package builds with zero type errors
- **SC-003**: All 8 UI components render correctly on both mobile (iOS + Android) and web environments, verified via Storybook + react-native-web (no native app required in Phase 3)
- **SC-005**: A consumer app can install both packages and render all components within 5 minutes (verified via a minimal test app)
- **SC-006**: Bundle size impact of importing all components is under 50KB gzipped
- **SC-007**: Dark mode toggle in the ThemeProvider switches all components within a single render cycle (no visual flash)
- **SC-008**: Full component library is documented with interactive examples showing every variant and state

## Assumptions

- Phase 2 backend services exist but are not consumed directly in Phase 3 (design system is platform-agnostic)
- The design system targets both Expo (React Native) and React (web) via a shared React-based architecture
- Map provider will use react-native-maps on mobile and Leaflet on web (tokens include map-specific colors only, no map components in Phase 3)
- The UI/UX Pro Max generated design system (`design-system/bornemap/MASTER.md`) provides the visual direction for token values
- TypeScript is the target language — all packages ship with `.d.ts` types
- The monorepo uses pnpm workspaces with packages under `source/front/packages/`
- Both packages follow semantic versioning independently
- Icons are out of scope for Phase 3 (will use Heroicons/Lucide in Phase 4 apps)
- Mobile and web driver apps are out of scope for Phase 3 — Phase 4 will consume these packages
