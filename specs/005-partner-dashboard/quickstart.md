# Quickstart: Partner Dashboard — Multi-Tenant Views

## Prerequisites

- All prior phases complete (backend CRUD + auth, admin portal, UI packages)
- Backend API running at `http://localhost:8080`
- Seed data loaded (5 partners, 100 stations, 300 chargers)
- Admin portal Phase 4 for partner user creation

## Setup

```bash
cd sources/frontend
pnpm install
```

## Development

```bash
# Start partner dashboard dev server
cd apps/partner-dashboard
pnpm dev
```

The partner dashboard is served at `http://localhost:5174` (Vite default for second app).

## Backend Changes Required

The following changes are needed on the backend to enable partner scoping:

1. **Auth middleware**: Add `partner_profile_id` to request extensions (lookup from `user_id` via `partner_profiles` table)
2. **Stations repository**: Add optional `owner_id` filter parameter to all list/detail queries
3. **Chargers repository**: Add station-join-based `owner_id` filter to list queries; verify station ownership on create/update/delete
4. **Partners handlers**: Add `GET /api/v1/partners/me` and `PATCH /api/v1/partners/me` endpoints (scoped, no ID param needed)
5. **Station handlers**: Ignore `owner_id` in request body on POST (auto-assign) and verify on PATCH/DELETE

## Validation Checklist

### 1. Auth & Access
- [ ] Login as a partner user — dashboard loads with Overview page
- [ ] Login as an admin user — redirected or blocked from partner dashboard
- [ ] Expired/invalid JWT — redirected to login page
- [ ] Two partner users from the same org see the same data

### 2. Overview Dashboard
- [ ] Landing page at `/` shows metric chips: total stations, total chargers
- [ ] Metrics reflect only the partner's own data
- [ ] Zero stations shows metric chip as 0 with empty state

### 3. Stations
- [ ] Stations page shows only the partner's own stations
- [ ] Create station — modal has no owner dropdown, owner auto-assigned
- [ ] Station appears in table and as marker on map
- [ ] Click station row — map pans to coordinates
- [ ] Click map marker — table row highlights
- [ ] Edit station — owner field hidden or read-only
- [ ] Delete station — ConfirmDeleteModal requires exact `STN-` ID
- [ ] Access another partner's station ID via URL — returns 403/404

### 4. Chargers
- [ ] Chargers page shows only chargers belonging to partner's stations
- [ ] Station filter dropdown lists only partner's own stations
- [ ] Create charger — station dropdown scoped to own stations
- [ ] Edit charger — status change reflected immediately
- [ ] Delete charger — ConfirmDeleteModal requires exact `CHG-` ID
- [ ] Station detail page shows only that station's chargers

### 5. Profile
- [ ] Profile page displays all fields: display_name, contact_phone, logo_url, classification, tax_id
- [ ] Classification and tax_id are read-only (disabled inputs)
- [ ] Display name, contact phone, logo URL are editable
- [ ] Changes persist after page reload

### 6. UI & Design
- [ ] Sidebar shows exactly 4 items: Overview, Stations, Chargers, Profile
- [ ] Admin-only routes (`/settings`, `/users`) return 403 or redirect
- [ ] Loading skeletons shown during API fetches
- [ ] Error states shown when API calls fail (no page crash)
- [ ] Empty states shown when partner has zero stations/chargers
- [ ] All tables use ScrollableTable pattern
- [ ] All destructive actions use ConfirmDeleteModal

## Test Commands

```bash
# Type-check
cd sources/frontend && pnpm -r type-check

# Lint
cd sources/frontend && pnpm -r lint

# Build
cd sources/frontend && pnpm -r build

# Backend tests
cd sources/backend && cargo test
```
