# Quickstart: Dashboard Admin View

**Date**: 2026-06-09

**Prerequisites**:
- Sprint 1.1 complete — json-server running with seeded data
- Node.js 18+, pnpm installed

## Setup

```bash
# Install dependencies
pnpm install

# Start the mock API (from repo root)
pnpm mock

# In a separate terminal, start the Dashboard dev server
pnpm dev:dashboard
```

The Dashboard App opens on `http://localhost:5173` (Vite default). All API calls go to `http://localhost:3001/api/*`.

## Verify Admin Screens

### Overview
- Open `http://localhost:5173`
- Three stat cards show total partners, stations, chargers
- Recent stations table loads below

### Partners
- Navigate to `/partners`
- Table shows 3 seeded partners with badges
- Click "Add Partner" → create a new partner
- Click Verify on PRT003 → badge flips to verified
- Click Deactivate → active toggle changes
- Click Edit → update name
- Click Delete → confirm and remove

### Stations
- Navigate to `/stations`
- Table shows 15 seeded stations
- Filter by partner → only that partner's stations
- Add a station → fill name, lat, lng, select partner
- Edit and delete work

### Chargers
- Navigate to `/chargers`
- Table shows 24 seeded chargers
- Filter by station → only that station's chargers
- Add a charger → select station, connector type, power, status
- Edit and delete work

### Dev Role Switcher
- Bottom of sidebar: toggle between Admin View and Partner View
- Partner View shows different navigation items
- Partner selector dropdown appears in Partner View
- Label shows "Dev Only — removed in MVP-3"

### Error Handling
- Stop `pnpm mock` → all screens show ErrorState with Retry
- Restart `pnpm mock` → click Retry → data loads again
- Open a screen with no data → EmptyState with create prompt shown

## Dev Commands

| Command | Description |
|---------|-------------|
| `pnpm mock` | Start json-server on port 3001 |
| `pnpm dev:dashboard` | Start Dashboard dev server on port 5173 |
| `pnpm dev` | List all available dev commands |

## Project Structure

```
source/apps/dashboard/
├── package.json
├── tsconfig.json
├── vite.config.ts
├── index.html
├── postcss.config.js
├── tailwind.config.js
└── src/
    ├── main.tsx
    ├── App.tsx
    ├── api/
    │   └── client.ts
    ├── context/
    │   └── RoleContext.tsx
    ├── components/
    │   ├── shared/
    │   │   ├── StatCard.tsx
    │   │   ├── DataTable.tsx
    │   │   ├── StatusBadge.tsx
    │   │   ├── Modal.tsx
    │   │   ├── EmptyState.tsx
    │   │   ├── ErrorState.tsx
    │   │   ├── Skeleton.tsx
    │   │   ├── Button.tsx
    │   │   └── Input.tsx
    │   └── layout/
    │       ├── AppShell.tsx
    │       ├── Sidebar.tsx
    │       ├── NavigationItem.tsx
    │       ├── TopBar.tsx
    │       └── PageContent.tsx
    └── pages/
        ├── Overview/
        │   ├── OverviewPage.tsx
        │   └── RecentStationsTable.tsx
        ├── Partners/
        │   ├── PartnersPage.tsx
        │   ├── PartnerTable.tsx
        │   └── PartnerForm.tsx
        ├── Stations/
        │   ├── StationsPage.tsx
        │   ├── StationTable.tsx
        │   └── StationForm.tsx
        └── Chargers/
            ├── ChargersPage.tsx
            ├── ChargerTable.tsx
            └── ChargerForm.tsx
```

## Expected Results

- Sidebar shows 4 admin nav items, active item is highlighted
- Overview stat cards show real counts from API
- Partner CRUD works end to end — create, verify, deactivate, edit, delete
- Station CRUD works with lat/lng validation
- Charger CRUD works with connector type and status management
- EmptyState shown when no data exists
- ErrorState with retry when API is down
- Dev role switcher toggles between admin and partner navigation
