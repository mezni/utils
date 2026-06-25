# Sprint 06 — Task Breakdown

## Phase 0: Foundation (branch + project scaffold)

- [ ] `0.1` Create branch `sprint/06-admin-dashboard-bootstrap`
- [ ] `0.2` Bootstrap Vite + React + TS project at `source/apps/admin-dashboard/`
  - `npm create vite@latest admin-dashboard -- --template react-ts`
  - Add `tailwindcss`, `postcss`, `autoprefixer`
  - Configure Tailwind + PostCSS
  - Add `react-router-dom`, `@tanstack/react-query`, `axios`
  - Configure `vite.config.ts` with proxy to admin-service (port 3002)
  - Set up path aliases (`@/` → `src/`)
- [ ] `0.3` Wire up root `App.tsx` with `QueryClientProvider`, `BrowserRouter`
- [ ] `0.4` Add global CSS for Tailwind + app-level styles
- [ ] `0.5` Create `src/lib/constants.ts` for route paths

## Phase 1: Layout Shell

- [ ] `1.1` Build `AppHeader.tsx` — title "BorneMap Admin" + no auth (placeholder user pill)
- [ ] `1.2` Build `AppSidebar.tsx` — Dashboard / Data (expandable, Partners, Stations, Chargers) / Settings links with active state
- [ ] `1.3` Build `ContentArea.tsx` — `<Outlet />` wrapper with padding
- [ ] `1.4` Build `AppShell.tsx` — composition of Header + Sidebar + ContentArea
- [ ] `1.5` Create `router.tsx` with all routes (lazy loaded pages)
- [ ] `1.6` Wire sidebar collapse state (localStorage persistence optional)

## Phase 2: Domain Types & Client Core

- [ ] `2.1` Add pagination DTO to `domain-types` (`PageParams`, `PaginatedResponse<T>`)
- [ ] `2.2` Add search params type to `domain-types` (`SearchParams`)
- [ ] `2.3` Ensure all entity CRUD hooks exist in `client-core`
- [ ] `2.4` Ensure all lookup table hooks exist in `client-core`

## Phase 3: Common Components

- [ ] `3.1` `SummaryCard.tsx` — icon, label, value, loading skeleton variant
- [ ] `3.2` `LoadingState.tsx` — centered spinner
- [ ] `3.3` `EmptyState.tsx` — icon + message + optional action button
- [ ] `3.4` `ErrorState.tsx` — error icon + message + retry button
- [ ] `3.5` `DataTable.tsx` — shadcn/ui Table wrapper with loading state
- [ ] `3.6` `SearchInput.tsx` — debounced search input
- [ ] `3.7` `ConfirmDialog.tsx` — confirm action dialog
- [ ] `3.8` `PageHeader.tsx` — title + breadcrumb + action button

## Phase 4: Dashboard Module

- [ ] `4.1` `useDashboardSummary.ts` — fetches counts from all 3 entities
- [ ] `4.2` `DashboardPage.tsx` — 3 summary cards in grid layout
- [ ] `4.3` Verify all 4 states (loading, empty, error, success)

## Phase 5: Partners Module

- [ ] `5.1` `PartnersPage.tsx` — search + table + create button
- [ ] `5.2` `usePartnersManager.ts` — hooks orchestration (list, delete)
- [ ] `5.3` `PartnersTable.tsx` — columns: id, name, email, website, status, actions
- [ ] `5.4` `PartnerFormDialog.tsx` — create/edit form in dialog
- [ ] `5.5` Verify all 4 states

## Phase 6: Stations Module

- [ ] `6.1` `StationsPage.tsx` — search + partner filter + table + create button
- [ ] `6.2` `useStationsManager.ts` — hooks orchestration
- [ ] `6.3` `StationsTable.tsx` — columns: id, name, address, partner, status, actions
- [ ] `6.4` `StationFormDialog.tsx` — create/edit form in dialog
- [ ] `6.5` Verify all 4 states

## Phase 7: Chargers Module

- [ ] `7.1` `ChargersPage.tsx` — search + station filter + table + create button
- [ ] `7.2` `useChargersManager.ts` — hooks orchestration
- [ ] `7.3` `ChargersTable.tsx` — columns: id, serial, connector type, status, actions
- [ ] `7.4` `ChargerFormDialog.tsx` — create/edit form in dialog
- [ ] `7.5` Verify all 4 states

## Phase 8: Settings Module

- [ ] `8.1` `SettingsPage.tsx` — tabs or sections for each lookup table
- [ ] `8.2` `LookupTableSection.tsx` — list + create/edit dialog for lookup values
- [ ] `8.3` Verify all 4 states for each table

## Phase 9: Polish & Validation

- [ ] `9.1` Accessibility audit (keyboard nav, aria labels, focus management)
- [ ] `9.2` Responsive layout check
- [ ] `9.3` Run build: `npm run build` (no errors)
- [ ] `9.4` Verify no blank screens anywhere

## Phase 10: Delivery

- [ ] `10.1` Commit `sprint/06-admin-dashboard-bootstrap`
- [ ] `10.2` Create PR

---

## Legend

- `[ ]` = pending
- `[x]` = completed
- `[~]` = in progress
