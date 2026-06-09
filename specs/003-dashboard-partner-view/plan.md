# Implementation Plan: Dashboard Partner View

**Branch**: `003-dashboard-partner-view` | **Date**: 2026-06-09 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/003-dashboard-partner-view/spec.md`

## Summary

Build the Dashboard App partner view — scoped CRUD screens for partners to manage their own stations, chargers, and availability. Leverages the existing AppShell, RoleContext, and shared components from Sprint 1.2. Adds data-scoping via selectedPartnerId from the dev role switcher context.

## Technical Context

**Language/Version**: TypeScript 5.7 (same as Sprint 1.2)

**Primary Dependencies**: React 18, React Router 6+, shared components from `source/apps/dashboard/src/components/`

**Storage**: N/A — data from json-server at `http://localhost:3001/api/*`

**Testing**: Manual verification against json-server (no test framework)

**Target Platform**: Web browser — Dashboard App (partner view)

**Project Type**: Extension of existing SPA — 4 new page components with data-scoping logic

**Performance Goals**: Screens load in under 2 seconds; switching partners immediately re-fetches scoped data; availability toggle responds in under 1 second

**Constraints**: All API calls use `/api` prefix; all visual values from shared design tokens; partner_id scoping via RoleContext.selectedPartnerId; no authentication; availability is append-only

**Scale/Scope**: 4 partner screens, 2 new API interactions (availability POST, station_availability filter), ~4 new page components

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| # | Principle | Check | Result |
|---|-----------|-------|--------|
| 1 | MVP-first delivery | Extends existing Dashboard — no new infrastructure | PASS |
| 2 | Layered complexity | Adds partner view on top of admin view without breaking anything | PASS |
| 3 | Dashboard first | Partner view is the second half of Dashboard — driver apps still to come | PASS |
| 4 | Single source of truth | json-server remains sole data source | PASS |
| 5 | Simple operations | Same `pnpm dev:dashboard` — no new commands | PASS |
| 9 | Visual consistency | All tokens from shared design system | PASS |
| 10 | API prefix consistency | All calls use `/api` prefix | PASS |
| — | Dev role switcher (Section 5.4) | Already labeled "Dev Only — removed in MVP-3" | PASS |
| — | Partner scope (Section 3.4) | Scoped via selectedPartnerId — full JWT enforcement in MVP-3 | PASS |

**Gate verdict**: ALL PASS — proceed to Phase 0

## Project Structure

### Documentation (this feature)

```text
specs/003-dashboard-partner-view/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
└── tasks.md             # Phase 2 output (separate command)
```

### Source Code (repository root)

```text
source/apps/dashboard/src/pages/
├── PartnerOverview/
│   └── PartnerOverviewPage.tsx
├── PartnerStations/
│   └── PartnerStationsPage.tsx
├── PartnerChargers/
│   └── PartnerChargersPage.tsx
└── PartnerAvailability/
    └── PartnerAvailabilityPage.tsx
```

All partner pages are new files. No existing files from Sprint 1.2 are modified — the Sidebar already has partner nav items (Overview, My Stations, My Chargers, Availability) from Sprint 1.2 US1, and RoleContext already provides `selectedPartnerId`.

**Structure Decision**: New page components co-located under `pages/` with existing admin pages. Each partner screen is a standalone React component wired into the existing AppShell via React Router.

## Complexity Tracking

No constitution violations — complexity tracking not required.

---

## Phase 0: Research

### Unknowns & Research Tasks

| # | Unknown | Research Task |
|---|---------|---------------|
| R01 | Data-scoping pattern for partner-specific API queries | Investigate how to filter stations by partner_id and chargers by partner's station IDs using the existing json-server filter API |
| R02 | Station availability current-status pattern | Investigate computing the latest station_availability record per station from an append-only log |
| R03 | Availability toggle UX — optimistic vs pessimistic update | Investigate whether to update immediately (optimistic) or wait for API confirmation (pessimistic) |
| R04 | Partner status bar design | Investigate how to display 3 flag states (verified/live/active) in a compact status bar format |

---

## Phase 1: Research Output

### R01 — Data-Scoping with RoleContext.selectedPartnerId

**Decision**: Each partner page reads `selectedPartnerId` from `useRole()`, passes it as `GET /api/stations?partner_id={id}` for station queries, and derives charger queries by collecting the partner's station IDs then filtering chargers.

**Rationale**: json-server supports `?partner_id=` filter queries natively. For chargers, two API calls are needed: first fetch partner's stations, then fetch `GET /api/chargers?station_id=STN001&station_id=STN002...`. json-server supports multiple values for the same query param.

### R02 — Latest Station Availability per Station

**Decision**: Fetch all station_availability records and compute the latest per station client-side by grouping by station_id and picking the max `updated_at`.

**Rationale**: station_availability is append-only. json-server does not support SQL-style `DISTINCT ON` or `GROUP BY`. Client-side grouping is simple (O(n) pass over the array) and the data set is small (one record per station per update, unlikely to exceed a few hundred records).

### R03 — Availability Toggle UX

**Decision**: Pessimistic update — disable the toggle, send POST, then refetch on success. Show error and revert on failure.

**Rationale**: The availability status must be accurately reflected in the UI since drivers depend on it. An optimistic update showing "Available" when the POST actually failed would be misleading. The toggle buttons are large enough that the 200-500ms API delay is imperceptible.

### R04 — Partner Status Bar

**Decision**: Three compact badge groups in a horizontal row: "Verified" / "Awaiting Verification", "Live" / "Not Live", "Active" / "Suspended". Each group uses the green/gray/red StatusBadge pattern from the shared components.

**Rationale**: Clear at-a-glance operational status. Three flags are independent — a partner can be Verified, Not Live, and Active simultaneously. The badge colors match the existing StatusBadge convention.
