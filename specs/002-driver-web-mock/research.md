# Research: Driver Web App with Mock Data

## Decisions

### i18n Library: react-i18next
- **Decision**: Use react-i18next with i18next
- **Rationale**: Industry standard for React i18n. Supports interpolation, pluralization, RTL detection, lazy loading. Seamless integration with React hooks (`useTranslation`). Works with TypeScript. Lighter than react-intl for this scope.
- **Alternatives considered**: react-intl (heavier, ICU message syntax), react-localized (less maintained), custom solution (unnecessary complexity)

### RTL Implementation
- **Decision**: `dir="rtl"` on `<html>` element when Arabic selected + Tailwind RTL variants (`rtl:` prefix)
- **Rationale**: Tailwind's RTL support with `rtl:` variants handles most layout reversal automatically. Combined with CSS logical properties (margin-inline-start, padding-inline-end) for fine control. No additional RTL library needed.
- **Alternatives considered**: CSS logical properties only (less ergonomic with Tailwind), custom RTL context (redundant)

### Router: React Router v6
- **Decision**: Use React Router v6 with createBrowserRouter
- **Rationale**: Standard React routing library. v6's data router API supports loaders/actions for future real API integration. Nested routes map naturally to layout structure (TopBar + Sidebar wrapper).
- **Alternatives considered**: TanStack Router (newer, less ecosystem), wouter (too minimal), reach-router (merged into RR)

### Mock Data Pattern
- **Decision**: TypeScript const objects with typed interfaces, exported as arrays
- **Rationale**: Type-safe, no build step needed, treeshakeable, easy to extend with future API types. Keeps mock data co-located with types for single-source type definitions.
- **Alternatives considered**: JSON files (no types), faker.js (unnecessary for structured mock data), MSW (too complex for this sprint, better suited for Phase 5)

### Testing Approach
- **Decision**: Vitest + @testing-library/react — same stack as Sprint 1.1
- **Rationale**: Consistency with existing project testing setup. Component tests for 9 driver-specific components. Screen-level integration tests for navigation and mock data rendering. No snapshot tests (high maintenance, low value for mock data sprint).
- **Alternatives considered**: Storybook (useful but adds setup overhead), Cypress (too heavy for unit-level), Playwright (better for e2e, planned for future)

### State Management
- **Decision**: React built-in state (useState, useContext) — no external state library
- **Rationale**: Mock data is read-only in this sprint. No complex state mutations. Simple context for favorites list. No need for Redux, Zustand, or TanStack Query.
- **Alternatives considered**: Zustand (overkill for mock data), TanStack Query (designed for real API), Redux (WAY too heavy)

### CSS Approach
- **Decision**: Tailwind CSS with `packages/ui/tailwind.config.base.js` extension
- **Rationale**: Design token consumption is required (Principle VIII). Tailwind utility classes for component styling. No CSS modules or styled-components needed.
- **Alternatives considered**: CSS Modules (loses token integration), styled-components (runtime overhead), vanilla CSS (maintenance burden)

## Dependencies to Install

- `react-router-dom` — Routing
- `react-i18next` + `i18next` — Internationalization
- `i18next-browser-languagedetector` — Auto language detection
- `@testing-library/react` + `@testing-library/jest-dom` + `@testing-library/user-event` — Testing
- `tailwindcss` + `postcss` + `autoprefixer` — CSS framework
- `vitest` + `@vitejs/plugin-react` — Test runner + Vite plugin
- `jsdom` — DOM environment for tests

## Best Practices

### Component Structure
- Each component receives props via TypeScript interface
- Components use Tailwind classes for styling via design tokens
- All interactive elements have ARIA labels
- Components test rendering variants, states, and accessibility

### Mock Data Structure
- Types defined first in `types/index.ts`
- Mock arrays typed as `TypeName[]` or `readonly TypeName[]`
- IDs follow NanoID-style prefixes (`STN-`, `CHG-`, `REV-`, `USR-`)
- Mock data co-located with types for easy future replacement

### Screen Structure
- Each screen is a standalone component in `screens/`
- Screens import components from `components/` and mock data from `mocks/`
- No screen-to-screen data sharing (state is local or via simple context)
- Future: replace mock imports with API hooks
