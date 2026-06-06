# Implementation Plan: Dashboard App with Mock Data

**Branch**: `004-dashboard-mock` | **Date**: 2026-06-06 | **Spec**: [spec.md](./spec.md)

**Input**: Sprint specification from `/specs/004-dashboard-mock/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/plan-template.md` for the execution workflow.

## Summary

Build a partner and admin dashboard application with mock data for the BorneMap EV charging platform. The dashboard provides role-based interfaces for partners to manage their station networks and for admins to oversee the entire platform. This sprint focuses on UI implementation with mock data, following the same Vite + React + TypeScript pattern established in Sprint 1.2 (Driver Web App). The dashboard will feature a sidebar navigation, role-aware routing, RTL support for Arabic, and data visualization components. All data will be sourced from local mock files, with no backend integration.

## Technical Context

**Language/Version**: TypeScript 5.7

**Primary Dependencies**: Vite 6, React 19, react-router-dom 7, Tailwind CSS 4, react-i18next, @borne-map/ui (workspace package)

**Storage**: Local mock data files (TypeScript), no database in this phase

**Testing**: Vitest (component tests), manual RTL verification

**Target Platform**: Modern web browsers (Chrome, Firefox, Safari) with JavaScript enabled

**Project Type**: web-application

**Performance Goals**: Overview screen loads within 3 seconds, role switching completes within 1 second

**Constraints**: WCAG 2.1 AA accessibility, Arabic RTL layout required on all screens, design tokens must be consumed from packages/ui

**Scale/Scope**: 13 screens (6 partner, 7 admin), 6 dashboard-specific components, mock data for 5 partners, 15 stations, 50+ chargers, 10 users, 60+ reviews

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Pragmatic Architecture | ✅ PASS | Single dashboard application - no new services added |
| II. Single Source of Truth | ✅ PASS | Mock data files are the source of truth for Phase 1 |
| III. Simple Operations | ✅ PASS | One-person operable web application with simple mock data |
| IV. Domain Separation by Schema | ✅ PASS | N/A - Phase 1 (UI only, no database yet) |
| V. Build for Current Scale | ✅ PASS | Mock data for realistic scale, no premature optimization |
| VI. Public Access First | ✅ PASS | N/A - Dashboard requires authentication (Phase 4) |
| VII. RTL & Arabic Built-In | ✅ PASS | RTL support specified in requirements, will be verified |
| VIII. Visual Consistency | ✅ PASS | All visual values from packages/ui design tokens |

**Non-Negotiable Rules Check**:

| Rule | Status | Notes |
|------|--------|-------|
| Arabic RTL layout MUST work correctly on every screen | ✅ PASS | FR-010 through FR-013 specify RTL requirements |
| All visual values from packages/ui | ✅ PASS | FR-019 requires token consumption |
| Tokens NOT stored in localStorage | ✅ PASS | N/A - Phase 1 (no auth yet) |
| Only Traefik exposes public network ports | ✅ PASS | N/A - Development environment |
| Keycloak owns all authentication | ✅ PASS | N/A - Mock auth in Phase 1 |

**All gates passed. Proceed to Phase 0 research.**

## Project Structure

### Documentation (this feature)

```text
specs/004-dashboard-mock/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
│   └── README.md        # Mock data contract documentation
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)

```text
apps/dashboard/
├── src/
│   ├── components/
│   │   ├── AppShell/
│   │   │   ├── AppShell.tsx
│   │   │   ├── Sidebar/
│   │   │   │   ├── Sidebar.tsx
│   │   │   │   ├── BrandHeader.tsx
│   │   │   │   ├── NavigationItem.tsx
│   │   │   │   └── BottomActions.tsx
│   │   │   └── TopBar.tsx
│   │   ├── PageContent/
│   │   │   └── PageContent.tsx
│   │   ├── DataCard/
│   │   │   └── DataCard.tsx
│   │   ├── DataTable/
│   │   │   ├── DataTable.tsx
│   │   │   └── table.types.ts
│   │   └── StatCard/
│   │   │   └── StatCard.tsx
│   ├── screens/
│   │   ├── OverviewScreen.tsx
│   │   ├── MyStationsScreen.tsx
│   │   ├── StationEditScreen.tsx
│   │   ├── ChargerManagementScreen.tsx
│   │   ├── AvailabilityUpdateScreen.tsx
│   │   ├── ReportsScreen.tsx
│   │   ├── UsersScreen.tsx
│   │   ├── PartnersScreen.tsx
│   │   ├── StationsScreen.tsx
│   │   ├── ChargersScreen.tsx
│   │   └── ReviewsScreen.tsx
│   ├── mocks/
│   │   ├── partners.ts
│   │   ├── stations.ts
│   │   ├── chargers.ts
│   │   ├── users.ts
│   │   ├── reviews.ts
│   │   └── reports.ts
│   ├── i18n/
│   │   ├── ar.json
│   │   ├── fr.json
│   │   └── index.ts
│   ├── hooks/
│   │   ├── useRole.ts
│   │   ├── useMockData.ts
│   │   └── useNavigation.ts
│   ├── context/
│   │   └── RoleContext.tsx
│   ├── types/
│   │   └── index.ts
│   ├── App.tsx
│   └── index.css
├── public/
│   └── favicon.ico
├── app.json
├── tsconfig.json
├── vite.config.ts
├── tailwind.config.js
└── package.json
```

**Structure Decision**: Web application structure using Vite for tooling, React for UI framework, and TypeScript for type safety. The dashboard follows the monorepo pattern established in Sprint 1.2 (Driver Web App), sharing the design tokens package (`packages/ui`) and similar configuration (Vite, Tailwind, i18n). Role-based routing and mock data management are handled via React Context and local TypeScript files.

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| (No violations found) | - | - |