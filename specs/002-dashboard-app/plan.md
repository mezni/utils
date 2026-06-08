# Implementation Plan: Dashboard App

**Branch**: `001-backend-and-database` | **Date**: June 8, 2026 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/002-dashboard-app/spec.md`

**Phase**: 1 (Design & Contracts)

## Summary

Build a partner/station/charger management dashboard using React + Vite + Tailwind, consuming the production-ready FastAPI backend from Sprint 1.1. The Dashboard is the entry point for partners to manage charging infrastructure end-to-end. Four screens: Overview (summary stats), Partners (full CRUD), Stations (full CRUD + location validation), Chargers (full CRUD + status badges). All screens fetch real data from `/api/v1/` endpoints, validate client-side, and update tables instantly on success without page reload.

---

## Technical Context

**Language/Version**: TypeScript 5.x (latest), React 18.x (latest), Node 18+

**Primary Dependencies**: 
- React 18.x, React Router v6
- Vite 5.x
- Tailwind CSS 3.x extending `source/packages/ui/tailwind.config.base.js`
- axios or fetch API (for HTTP requests)
- React Hook Form or similar (for form state management)
- shadcn/ui or Headless UI (for modals, dropdowns, buttons)

**Storage**: PostgreSQL 15 (via FastAPI backend at http://localhost:8000). Frontend is stateless; all data persists server-side.

**Testing**: Vitest (unit), React Testing Library (component), optional E2E (Playwright/Cypress for full loop)

**Target Platform**: Web browsers (Chrome, Firefox, Safari), desktop only (mobile dashboard out of scope)

**Project Type**: Single Page Application (SPA) — React frontend consuming REST API

**Performance Goals**: 
- Form submission → table update: <1 second (SC-001 to SC-005)
- Initial page load: <3 seconds (empty state + skeleton loaders)
- Filter dropdown population: <500ms from API call
- No visual regressions across browsers

**Constraints**: 
- Zero hardcoded colors/spacing; all from design tokens (`source/packages/ui/src/tokens/colors.ts`)
- Coordinate validation: latitude -90 to 90, longitude -180 to 180
- API error handling without console errors
- Graceful degradation when API is unreachable

**Scale/Scope**: 
- 4 main screens (Overview, Partners, Stations, Chargers)
- ~15 Tunisian stations seeded at launch
- 3-5 partners (pilot data)
- No pagination required for MVP-1
- Single-user workflow (no real-time sync)

---

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

**Compliance with BorneMap Constitution**:

✅ **API Prefix Rule** — All API calls use `/api/v1/` prefix (documented in backend Sprint 1.1)

✅ **Public Access First** — Dashboard is partner-admin-only (auth deferred to MVP-3), but backend endpoints are public; no blocking

✅ **Single Source of Truth** — Data persists exclusively in PostgreSQL `inventory` schema via FastAPI service; frontend is read-only replica

✅ **Design Tokens** — All colors, spacing, typography sourced from `source/packages/ui/src/tokens/colors.ts` and extended Tailwind config

✅ **Identifiers** — Uses UUID from backend; MVP-1 requirement

✅ **No Hardcoded Visual Values** — All styling via tokens + Tailwind; no inline styles

✅ **Zero Duplication of Business Logic** — Forms validate client-side for UX; server validates server-side; separation of concerns

**No constitution violations identified.** Dashboard implements partner-controlled data management as intended by MVP-1 design.

---

## Project Structure

### Documentation (this feature)

```text
specs/002-dashboard-app/
├── spec.md                          # Feature specification
├── plan.md                          # This file (Phase 1 output)
├── research.md                      # Phase 0 output (if research needed)
├── data-model.md                    # Phase 1 output
├── quickstart.md                    # Phase 1 output
├── contracts/
│   └── api-integration.md           # API contract with backend
├── checklists/
│   └── requirements.md              # Quality checklist
└── tasks.md                         # Phase 2 output (future)
```

### Source Code (repository root)

```text
source/apps/dashboard/                          # NEW - Dashboard application root
├── src/
│   ├── components/
│   │   ├── AppShell/                          # Main layout wrapper
│   │   │   ├── Sidebar.tsx                    # Left nav sidebar
│   │   │   ├── TopBar.tsx                     # Top header bar
│   │   │   └── Layout.tsx                     # Container component
│   │   ├── Common/
│   │   │   ├── DataTable.tsx                  # Reusable table component
│   │   │   ├── StatCard.tsx                   # Overview stat display
│   │   │   ├── Modal.tsx                      # Form modal wrapper
│   │   │   ├── ErrorState.tsx                 # API error display
│   │   │   ├── EmptyState.tsx                 # Empty data display
│   │   │   ├── LoadingSkeletons.tsx           # Table/card loaders
│   │   │   └── StatusBadge.tsx                # Charger status display
│   │   ├── Forms/
│   │   │   ├── PartnerForm.tsx                # Partner create/edit
│   │   │   ├── StationForm.tsx                # Station create/edit
│   │   │   └── ChargerForm.tsx                # Charger create/edit
│   │   └── Screens/
│   │       ├── Overview.tsx                   # Overview with stats
│   │       ├── PartnersScreen.tsx             # Partner management
│   │       ├── StationsScreen.tsx             # Station management
│   │       └── ChargersScreen.tsx             # Charger management
│   ├── services/
│   │   ├── api.ts                             # Axios/fetch client
│   │   ├── partners.ts                        # Partner API calls
│   │   ├── stations.ts                        # Station API calls
│   │   └── chargers.ts                        # Charger API calls
│   ├── hooks/
│   │   ├── usePartners.ts                     # Partner data fetching
│   │   ├── useStations.ts                     # Station data fetching
│   │   ├── useChargers.ts                     # Charger data fetching
│   │   └── useForm.ts                         # Form submission logic
│   ├── types/
│   │   ├── api.ts                             # TypeScript types from API
│   │   ├── forms.ts                           # Form state types
│   │   └── models.ts                          # Domain models
│   ├── utils/
│   │   ├── validation.ts                      # Client-side validators
│   │   ├── formatters.ts                      # Date/number formatting
│   │   └── errors.ts                          # Error message helpers
│   ├── App.tsx                                # Root component + routing
│   ├── main.tsx                               # Entry point
│   └── index.css                              # Global styles (tokens)
├── public/
│   └── [static assets]
├── tests/
│   ├── unit/
│   │   ├── components/
│   │   ├── services/
│   │   └── utils/
│   └── integration/
│       ├── partners.test.tsx
│       ├── stations.test.tsx
│       └── chargers.test.tsx
├── vite.config.ts                             # Vite configuration
├── tsconfig.json                              # TypeScript config
├── tailwind.config.js                         # Extends base config
├── postcss.config.js                          # PostCSS for Tailwind
├── package.json
├── .env.example                               # Template: VITE_API_BASE_URL=http://localhost:8000
├── .gitignore
└── README.md                                  # Dashboard-specific docs

source/packages/ui/                            # EXISTING - Shared design tokens
├── src/tokens/
│   ├── colors.ts                              # Color token definitions
│   └── native.ts                              # React Native equivalents
└── tailwind.config.base.js                    # Base Tailwind config to extend
```

---

## Development Phases

### Phase 0: Research *(if needed)*

**Clarifications to resolve** (from spec and technical context):

1. **Form State Library**: React Hook Form vs useState vs Formik?
   - Decision: Use React Hook Form (lightweight, good TypeScript support, minimal re-renders)

2. **Component Library**: shadcn/ui vs Headless UI vs Material-UI?
   - Decision: shadcn/ui (built on Headless UI, uses Tailwind, highly customizable with tokens)

3. **HTTP Client**: axios vs fetch vs TanStack Query?
   - Decision: axios + custom hooks for data fetching (simple, familiar, sufficient for MVP-1)

4. **State Management**: Context API vs Redux vs Zustand?
   - Decision: Context API + custom hooks (minimal app size, no need for global state in MVP-1)

**Research Tasks** (if Phase 0 needed):
- Confirm React 18 + Vite 5 + TypeScript 5 compatibility
- Validate Tailwind extending from base config works correctly
- Test form validation library choice with coordinate range validation
- Confirm shadcn/ui components work with inherited tokens

**Output**: research.md (if Phase 0 executed)

---

### Phase 1: Design & Contracts *(IN PROGRESS)*

#### 1a. Data Model

**Output**: data-model.md with:
- Entity relationships (Partner → Station → Charger)
- Field validation rules (lat/lon ranges, required fields, status enum)
- API response types from backend
- Form input types (create vs edit payloads)

#### 1b. API Contracts

**Output**: contracts/api-integration.md with:
- All 16 endpoints from Sprint 1.1 backend
- Request/response types
- Error codes and handling strategies
- Example curl commands and TypeScript fetch patterns

#### 1c. Component Architecture

**Key Components**:

- **AppShell** — Fixed left sidebar + top bar + scrollable content
  - Sidebar: 256px fixed width, white bg, border-right
  - Active nav item: bg-`brand.sageLight`, text-`brand.primary`
  - TopBar: 64px fixed height, white bg, border-bottom
  
- **DataTable** — Reusable table component
  - Headers, rows, actions column (edit, delete)
  - Loading skeleton while fetching
  - Empty state when no data
  
- **Modal** — Form wrapper
  - Overlay, centered card, close button
  - Submit/Cancel buttons
  - Form validation errors inline
  
- **StatCard** — Stat display
  - Label, count, optional icon
  - Fetches from API on mount
  
- **StatusBadge** — Charger status visual
  - available = green (#10B981)
  - in_use = amber (#F59E0B)
  - maintenance = red (#EF4444)

#### 1d. Routing Structure

```
/ → Overview (dashboard summary)
/partners → Partners management
/stations → Stations management
/chargers → Chargers management
```

#### 1e. Form Validation Rules

**Partner Form**:
- name: required, min 1 char, max 255 chars

**Station Form**:
- name: required, 1-255 chars
- address: required, 1-500 chars
- latitude: required, number, -90 to 90
- longitude: required, number, -180 to 180
- partner_id: required, UUID format

**Charger Form**:
- station_id: required, UUID
- connector_type: required, one of [Type2, CCS, CHAdeMO, J1772, etc.]
- power_kw: required, positive number
- status: required, one of [available, in_use, maintenance]

#### 1f. Error Handling Strategy

- **Network Error** (API unreachable): ErrorState with retry button on all screens
- **Validation Error** (422): Inline field error messages, form stays open
- **Not Found** (404): Graceful message, return to list
- **Server Error** (500): Generic error toast, log to console
- **Form Submission Failure**: Preserve form data, show error, allow retry

#### 1g. Agent Context Update

Update AGENTS.md with pointer to this plan.md file between `<!-- SPECKIT START -->` and `<!-- SPECKIT END -->` markers.

**Output**: Updated AGENTS.md with:
```
<!-- SPECKIT START -->
**Current Feature**: Dashboard App (Sprint 1.2)
**Feature Directory**: `specs/002-dashboard-app/`
**Plan Files**: plan.md, data-model.md, contracts/, quickstart.md
<!-- SPECKIT END -->
```

#### 1h. Quickstart Guide

**Output**: quickstart.md with:
- Dev environment setup (Node 18+, npm/yarn)
- Installation: `npm install`
- Running: `npm run dev` (Vite dev server on port 5173)
- Building: `npm run build`
- Testing: `npm run test`
- Connecting to backend: `VITE_API_BASE_URL=http://localhost:8000`
- Creating first partner: step-by-step walkthrough

---

## Deliverables

### Phase 1 Outputs

1. **data-model.md** — Entity definitions, relationships, validation rules
2. **contracts/api-integration.md** — API contract with backend, request/response types
3. **quickstart.md** — Developer setup and first-run experience
4. **Updated AGENTS.md** — Agent context with plan reference

### Phase 2 Outputs (subsequent `/speckit.tasks` command)

5. **tasks.md** — Atomic implementation tasks (estimated ~50-80 tasks across 4 sprints)

---

## Exit Criteria (Phase 1)

- ✅ Technical context fully specified (no NEEDS CLARIFICATION)
- ✅ Data model and relationships documented
- ✅ API contracts frozen and documented
- ✅ Component architecture defined
- ✅ Form validation rules specified
- ✅ Error handling strategy documented
- ✅ Routing structure mapped
- ✅ Development quickstart guide complete
- ✅ Agent context updated with plan reference
- ✅ Ready for Phase 2 task generation

---

## Timeline Estimate

- **Design Phase (Phase 1)**: 1 day (this plan, data model, contracts)
- **Implementation (Phase 2)**: 2 weeks (4 screens, CRUD operations, validation, error handling)
- **Testing & Integration (Phase 3)**: 1 week (cross-browser testing, full loop verification)
- **Total**: ~2 weeks for complete Dashboard delivery

---

## Next Steps

1. Execute `/speckit.tasks` to generate atomic implementation tasks
2. Begin implementation of AppShell, data-fetching hooks, and Common components
3. Implement Partner screen (simplest CRUD) first
4. Implement Station screen (adds location validation)
5. Implement Charger screen (adds status management)
6. Implement Overview screen
7. Full loop testing: create partner → station → charger in Dashboard, verify on Driver apps
8. Cross-browser testing and visual QA
9. Submit PR for review

---

**Status**: READY FOR TASK GENERATION (Phase 2)
