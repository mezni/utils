# Research: Dashboard Admin View

## R01 — React Router Layout Routes for Role-Based Navigation

**Decision**: Use React Router 6 layout routes with `<Outlet>` for the AppShell, nested index routes for each admin screen.

**Rationale**: React Router 6's layout route pattern renders the sidebar + top bar once and swaps the `<Outlet>` content per screen. Role-based filtering of nav items is driven by `RoleContext` state — the nav item array is filtered client-side when the dev role switcher toggles. No route-level guards needed since both role views live in the same app.

**Alternatives considered**: Separate route configs per role (duplicated), single flat route list with role checks in each screen (repetitive), dynamic `react-router` config generation (over-engineered for 5 screens).

## R02 — DataTable with CRUD Actions

**Decision**: Build a reusable `DataTable` component that accepts columns config (key, label, render function, sortable flag) and data array. An `actions` column renders edit/delete buttons per row.

**Rationale**: Each admin screen follows the same pattern — fetch data → display in table → inline actions. A single `DataTable` component with configurable columns avoids 4 near-identical table implementations. The render function pattern allows type badges, status badges, and toggle switches in columns.

**Alternatives considered**: Third-party table library (unnecessary dep for ~4 tables), per-screen hardcoded tables (duplication), TanStack Table (config-based but adds dependency).

## R03 — Modal CRUD Form Pattern

**Decision**: Single `Modal` component accepting `isOpen`, `onClose`, title, and children. Each CRUD form lives in its own component (e.g., `PartnerForm`, `StationForm`) that receives initial values and an `onSubmit` callback. State management: local `useState` in the page component for modal visibility and selected item.

**Rationale**: Simple and predictable. The Modal is a generic shell; forms are standalone components with their own validation. This keeps each form self-contained and testable. Validation runs on submit — required fields show inline red text below the field.

**Alternatives considered**: Form library like React Hook Form (adds dependency for simple forms), global modal state in context (over-engineered — each screen manages exactly one modal at a time).

## R04 — React Context for Dev Role Switcher

**Decision**: `RoleContext` provider wrapping the entire app at the AppShell level. Stores `role` (admin | partner) and `selectedPartnerId` (string | null). `Sidebar` reads context to filter nav items. Data-fetching hooks read context to scope queries.

**Rationale**: The dev role switcher toggle and partner selector are in the sidebar. Every screen needs access to the current role and selected partner. Context avoids prop drilling through AppShell → PageContent → individual pages. State is lost on page reload (intentional — the dev switcher is a session-only dev tool).

**Alternatives considered**: URL params for role/partner (leaks dev-only state into URLs), localStorage (persists dev state across sessions — not desired), Redux/Zustand (overkill for two values).

## R05 — Error Handling Pattern for JSON Server

**Decision**: Centralized `fetchWithError` wrapper around `fetch` that catches network errors and non-OK responses. Each page manages its own `{ loading, error, data }` state. `ErrorState` component renders with a "Retry" button that re-calls the fetch function.

**Rationale**: json-server can be stopped/started independently. When it stops, every fetch fails. A wrapper ensures consistent error object shape. Per-page error state is simpler than a global error boundary — each screen can independently show ErrorState while others continue working. The retry button just re-executes the fetch.

**Alternatives considered**: React Error Boundary (catches render errors, not fetch errors), global toast-based errors (loses the screen-specific UX requirement for inline ErrorState), single `useApi` hook (too coupled — different pages need different caching/fetch patterns).

## R06 — Tailwind + Shared Tokens Integration

**Decision**: Dashboard `tailwind.config.js` imports `source/packages/ui/tailwind.config.base.js` as a `preset`. The preset defines all theme extensions (colors, fonts, spacing, radius, shadows). The dashboard config adds the `content` paths for its own source files.

**Rationale**: Tailwind presets merge the base theme extensions into the consuming app's config. This is the idiomatic Tailwind way to share theme values across projects. The dashboard app adds its own `content: ['./src/**/*.{ts,tsx}']` to scan its source files, while the preset handles the token definitions.

**Implementation**:
```js
// tailwind.config.js
const base = require('../../packages/ui/tailwind.config.base');
module.exports = {
  presets: [base],
  content: ['./src/**/*.{ts,tsx}'],
};
```

**Alternatives considered**: CSS custom properties (loses Tailwind intellisense), importing the TS tokens directly (requires build-time token extraction), duplicating values (violates Principle 9 — no hardcoded visual values).
