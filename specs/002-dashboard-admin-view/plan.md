# Implementation Plan: Dashboard Admin View

**Branch**: `002-dashboard-admin-view` | **Date**: 2026-06-09 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/002-dashboard-admin-view/spec.md`

## Summary

Build the Dashboard App admin view — a React + Vite application that connects to the existing json-server mock API. Deliver a fixed sidebar layout with navigation, dev role switcher, shared component library, and four admin CRUD screens (Overview, Partners, Stations, Chargers) with full error handling.

## Technical Context

**Language/Version**: TypeScript 5.7

**Primary Dependencies**: React 18, React Router 6+, Vite 5+, Tailwind CSS 3.4+, shared tokens from `source/packages/ui`

**Storage**: N/A — data fetched from json-server mock API at `http://localhost:3001/api/*`

**Testing**: Manual verification against json-server (no test framework this sprint — per Sprint 1.1 convention in implementation plan)

**Target Platform**: Web browser (Chrome, Firefox, Safari — desktop-focused for MVP-1 Dashboard)

**Project Type**: Single-page application (Dashboard App, admin role)

**Performance Goals**: Screens load in under 2 seconds on local dev environment; no loading spinners visible for more than 1s on API fetch

**Constraints**: All API calls under `/api` prefix; all visual values from shared design tokens (no hardcoded colors/spacing); no authentication; dev role switcher labeled "Dev Only — removed in MVP-3"; lat range -90 to 90, lng -180 to 180

**Scale/Scope**: 5 admin screens, 4 API resources, ~12 shared components, ~27 data table columns

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| # | Principle | Check | Result |
|---|-----------|-------|--------|
| 1 | MVP-first delivery | Builds on Sprint 1.1 mock API; no unnecessary infrastructure | PASS |
| 2 | Layered complexity | Adds Dashboard on top of existing mock API without breaking anything | PASS |
| 3 | Dashboard first | Dashboard built before driver apps — matches build order | PASS |
| 4 | Single source of truth | Mock API (json-server) is sole data source; Dashboard is read-only consumer | PASS |
| 5 | Simple operations | Single `pnpm dev:dashboard` command; no build orchestration needed | PASS |
| 6 | Domain separation | N/A (MVP-1, no schema separation yet) | N/A |
| 7 | Public access | N/A (Dashboard is admin-only tool) | N/A |
| 8 | RTL / Arabic | N/A (RTL deferred to MVP-3) | N/A |
| 9 | Visual consistency | All tokens from `source/packages/ui/tailwind.config.base.js` | PASS |
| 10 | API prefix consistency | All fetch calls use `/api` prefix | PASS |
| 11 | Tooling separation | Code = SpecKit, UX = Impeccable, planning = this assistant | PASS |
| — | Dev role switcher (Section 5.4) | Labeled "Dev Only — removed in MVP-3" | PASS |
| — | Dashboard layout (Section 6.6) | Fixed sidebar w-64, TopBar h-16, `surface.background` content | PASS |
| — | Dashboard-specific components (Section 6.7) | AppShell, Sidebar, NavigationItem, TopBar, PageContent, DataCard, DataTable | PASS |

**Gate verdict**: ALL PASS — proceed to Phase 0

## Project Structure

### Documentation (this feature)

```text
specs/002-dashboard-admin-view/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
└── contracts/           # Phase 1 output
```

### Source Code (repository root)

```text
source/
└── apps/
    └── dashboard/
        ├── package.json
        ├── tsconfig.json
        ├── vite.config.ts
        ├── index.html
        ├── postcss.config.js
        ├── tailwind.config.js    # extends packages/ui/tailwind.config.base.js
        └── src/
            ├── main.tsx          # Entry point, React Router setup
            ├── App.tsx            # Layout wrapper
            ├── api/
            │   └── client.ts     # fetch wrapper with /api prefix
            ├── context/
            │   └── RoleContext.tsx # Dev role switcher + partner selection state
            ├── components/
            │   ├── shared/       # StatCard, DataTable, StatusBadge, Modal,
            │   │                  # EmptyState, ErrorState, Skeleton, Button, Input
            │   └── layout/       # AppShell, Sidebar, NavigationItem, TopBar, PageContent
            └── pages/
                ├── Overview/
                ├── Partners/
                ├── Stations/
                └── Chargers/
```

**Structure Decision**: Single application under `source/apps/dashboard/` following standard Vite + React conventions. Shared components co-located in the app (not extracted to `packages/ui`) since Sprint 1.7 Hardening may promote them later.

## Complexity Tracking

No constitution violations identified — complexity tracking not required.

---

## Phase 0: Research

### Unknowns & Research Tasks

| # | Unknown | Research Task |
|---|---------|---------------|
| R01 | React Router setup pattern for role-based navigation | Investigate layout routes with nested index routes for Sidebar + TopBar shell, role-based filter on nav items |
| R02 | DataTable component pattern for CRUD with inline actions | Investigate table with action column, row click, sort, filter, pagination patterns |
| R03 | Modal-based CRUD form pattern with validation | Investigate modal open/close state management, form validation UX, optimistic vs pessimistic updates |
| R04 | React Context pattern for dev role switcher state | Investigate context provider wrapping AppShell, consumed by Sidebar and by data-fetching hooks |
| R05 | Error handling pattern for json-server failures | Investigate fetch wrapper with try/catch, centralized error state, retry-from-last-action pattern |
| R06 | Tailwind + shared tokens integration | Investigate how `tailwind.config.js` extends `tailwind.config.base.js` — preset and content paths |
