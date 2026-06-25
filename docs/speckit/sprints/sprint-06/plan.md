# Sprint 06 — Architecture & Implementation Plan

## 1. High-Level Architecture

```
admin-dashboard (Vite + React 18 + TS)
  │
  ├── ui-kit (shadcn/ui components)
  ├── domain-types (Zod schemas, DTOs)
  ├── client-core (TanStack Query hooks + axios instance)
  │
  └── admin-service (API port 3002, NO CHANGES)
```

### Package Dependency Chain (Constitution §7.3)
```
ui-kit ← domain-types ← client-core ← admin-dashboard
```
- `admin-dashboard` imports from `client-core`, `domain-types`, `ui-kit`
- `client-core` imports from `domain-types`
- `domain-types` is standalone (Zod schemas only)
- `ui-kit` is standalone (no business logic)

## 2. Application Structure

```
source/apps/admin-dashboard/
├── index.html
├── package.json
├── tsconfig.json
├── vite.config.ts
├── tailwind.config.ts
├── postcss.config.js
├── public/
└── src/
    ├── main.tsx                    # Entry point
    ├── App.tsx                     # Router + QueryClientProvider
    ├── lib/
    │   └── constants.ts            # Route paths, labels
    ├── components/
    │   ├── ui/                     # shadcn/ui components (via ui-kit re-export)
    │   ├── layout/
    │   │   ├── AppShell.tsx        # Header + Sidebar + Content layout
    │   │   ├── AppHeader.tsx
    │   │   ├── AppSidebar.tsx
    │   │   └── ContentArea.tsx
    │   ├── summary-cards/
    │   │   └── SummaryCard.tsx
    │   └── common/
    │       ├── LoadingState.tsx
    │       ├── EmptyState.tsx
    │       ├── ErrorState.tsx
    │       ├── DataTable.tsx       # Wrapper around shadcn/ui Table
    │       ├── SearchInput.tsx
    │       ├── ConfirmDialog.tsx
    │       └── PageHeader.tsx
    ├── features/
    │   ├── dashboard/
    │   │   ├── DashboardPage.tsx
    │   │   └── useDashboardSummary.ts
    │   ├── partners/
    │   │   ├── PartnersPage.tsx
    │   │   ├── PartnerFormDialog.tsx
    │   │   ├── PartnersTable.tsx
    │   │   └── usePartnersManager.ts
    │   ├── stations/
    │   │   ├── StationsPage.tsx
    │   │   ├── StationFormDialog.tsx
    │   │   ├── StationsTable.tsx
    │   │   └── useStationsManager.ts
    │   ├── chargers/
    │   │   ├── ChargersPage.tsx
    │   │   ├── ChargerFormDialog.tsx
    │   │   ├── ChargersTable.tsx
    │   │   └── useChargersManager.ts
    │   └── settings/
    │       ├── SettingsPage.tsx
    │       └── LookupTableSection.tsx
    └── styles/
        └── global.css
```

## 3. Data Flow

```
Browser
  │
  ▼
React Router → Feature Page → useXxxManager hook
  │                                │
  ▼                                ▼
QueryClientProvider         client-core hooks (usePartners, etc.)
  │                                │
  ▼                                ▼
TanStack Query Cache       axios instance → admin-service API
                                    │
                                    ▼
                              PostgreSQL Database
```

### State Management
- **Server state**: TanStack Query (caching, refetching, mutations)
- **UI state**: React useState/useReducer (local form state, dialogs)
- **No global client state library** needed

## 4. Key Implementation Decisions

### Decision 1: Form Dialog
Use shadcn/ui Dialog + React Hook Form (if needed) for CRUD forms.
Keep it simple — controlled form state with validation via Zod schemas.

### Decision 2: Soft Delete
Admin-service uses `deleted_at` soft-delete. UI sends DELETE request and invalidates query cache.

### Decision 3: Search & Pagination
Admin-service supports `?q=search&page=1&per_page=20`.
UI implements search input with debounce + TanStack Query keepPreviousData.

### Decision 4: Sidebar State
Collapsible sidebar with active route highlighting using `useLocation()`.

## 5. Branch Strategy

```
sprint/05-admin-service-crud (completed)
  └── sprint/06-admin-dashboard-bootstrap (current)
```

## 6. Verification Criteria

1. `npm run dev` starts without errors
2. Dashboard shows summary cards with live data
3. Partners page: list, search, create, edit, soft-delete all work
4. Stations page: list, filter by partner, CRUD works
5. Chargers page: list, filter by station, CRUD works
6. Settings page: all 5 lookup tables CRUD works
7. All states render: Loading/Empty/Success/Error
8. Keyboard navigation works
9. No blank screens
