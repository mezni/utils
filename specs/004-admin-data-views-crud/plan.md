# Implementation Plan: Admin Data Views & CRUD

**Branch**: `008-admin-data-views-crud` | **Date**: 2026-05-26 | **Spec**: specs/004-admin-data-views-crud/spec.md

**Input**: Feature specification from `specs/004-admin-data-views-crud/spec.md`

## Summary

Build the Admin Portal data management section: full CRUD for Partners, Stations, Chargers, and Connector Types within the existing AppShell layout. Each entity gets a scrollable table listing, modal create/edit forms, and a confirmation modal requiring exact ID match for deletion. Stations page adds bidirectional map-table interaction. Chargers are accessible both via flat `/data/chargers` list and nested under station detail. Connector Types in Settings demonstrate cross-workspace dropdown dependency.

## Technical Context

**Language/Version**: TypeScript 5.x (React 18.x, JSX)

**Primary Dependencies**: React Router v6 (navigation/routing), react-leaflet v4 (map interaction), Tailwind CSS v3 (styling via design tokens), @bornemap/ui (ScrollableTable, SettingsCard, SelectSetting, ConfirmDeleteModal, MetricChip)

**Storage**: In-memory React state + local component state; data fetched via fetch() from `/api/v1/*` backend endpoints

**Testing**: No test framework specified for MVP0 frontend (Phase 7 handles manual validation)

**Target Platform**: Modern web browsers (Chrome, Firefox, Safari, Edge)

**Project Type**: Single-page web application (React + Vite)

**Performance Goals**: Page transitions <500ms perceived; API responses rendered within 200ms of receipt; map pan/zoom at 60fps

**Constraints**: No hardcoded hex colors (must use design tokens from tailwind.config.cjs); all data tables use `<ScrollableTable/>` with min-width 800px; all destructive actions require `<ConfirmDeleteModal/>` with exact ID match; soft-delete entities must not appear in lists after deletion

**Scale/Scope**: Seed data ~100 stations, ~300 chargers across ~5 partners; tables use scroll-within-container (no pagination for MVP0) based on seed dataset size

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Gate | Status |
|-----------|------|--------|
| I. Modular Monorepo Architecture | All code under `sources/frontend/apps/admin-portal/`; no cross-app coupling | ✅ PASS — admin-portal already exists as dedicated workspace |
| II. Semantic Identity & Data Isolation | All IDs match `PRT-`/`STN-`/`CHG-`/`CNT-` prefix format; `is_test` respected in queries | ✅ PASS — backend enforces; frontend displays as-is |
| III. Administrative UX Discipline | `<ScrollableTable/>` on all data matrices; `<ConfirmDeleteModal/>` with typed ID; design tokens only; sandbox border-t-4 | ✅ PASS — all components already exist in @bornemap/ui |
| IV. Mobile & Discovery Constraints | N/A — Admin Portal only, no mobile discovery in this feature | ✅ N/A |
| V. Deterministic Implementation | Domain layers modular; uses existing component patterns | ✅ PASS — reuses established component architecture |

**Post-Design Re-check**: All gates still pass. No design decisions introduced new violations.

## Project Structure

### Documentation (this feature)

```text
specs/004-admin-data-views-crud/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
│   └── api.md
├── checklists/
│   └── requirements.md  # Spec quality checklist
└── spec.md              # Feature specification
```

### Source Code (repository root)

```text
sources/frontend/
├── apps/admin-portal/src/
│   ├── components/
│   │   ├── layout/           # Existing: app-shell, sidebar-nav, header
│   │   └── data/             # NEW: data-view components
│   │       ├── partners-table.tsx
│   │       ├── stations-table.tsx
│   │       ├── chargers-table.tsx
│   │       ├── connector-types-table.tsx
│   │       ├── partner-form-modal.tsx
│   │       ├── station-form-modal.tsx
│   │       ├── charger-form-modal.tsx
│   │       └── connector-type-form-modal.tsx
│   ├── pages/
│   │   ├── overview.tsx      # Existing
│   │   ├── users.tsx         # Existing placeholder
│   │   ├── data/             # NEW: data section pages
│   │   │   ├── partners.tsx
│   │   │   ├── stations.tsx
│   │   │   └── chargers.tsx
│   │   ├── analytics.tsx     # Existing placeholder
│   │   ├── security.tsx      # Existing placeholder
│   │   └── settings/
│   │       ├── index.tsx     # Existing placeholder
│   │       └── infrastructure-types.tsx  # NEW
│   └── routes.tsx or App.tsx # Updated with new routes
│
└── packages/ui/src/
    └── components/ui/
        ├── scrollable-table.tsx   # Existing
        ├── settings-card.tsx      # Existing
        ├── select-setting.tsx     # Existing
        ├── confirm-delete-modal.tsx # Existing
        └── metric-chip.tsx        # Existing
```

**Structure Decision**: Standard React SPA with page components in `pages/` and reusable data-display components in `components/data/`. Follows the established pattern from Phase 3 (pages directory per route, components directory for shared UI pieces).

## Complexity Tracking

> No constitution violations — this feature fits entirely within the established architecture. Complexity Tracking section not applicable.

