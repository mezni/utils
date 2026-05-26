# Quickstart: Admin Data Views & CRUD

## Prerequisites

- Phase 3 Admin Portal Shell is fully implemented (AppShell, routes, BaseMap, design system components)
- Backend Phase 1 CRUD endpoints are deployed and accessible at `/api/v1/*`
- Seed data loaded (5 partners, 100 stations, 300 chargers, 5 connector types)

## Validation Checklist

### Partners (Data → Partners)

- [ ] Partners table loads with correct columns: ID, Display Name, Classification, Tax ID, Contact Phone, Created
- [ ] Create Partner modal opens with classification toggle (Business/Private)
- [ ] Toggling to "Business" shows Tax ID field; toggling to "Private" hides it
- [ ] Creating a partner succeeds and new row appears in table
- [ ] Editing a partner via modal updates the row
- [ ] Delete opens `<ConfirmDeleteModal/>` — button disabled until exact `PRT-` ID typed
- [ ] After delete, partner no longer appears in table
- [ ] Empty state shown when no partners exist

### Stations (Data → Stations)

- [ ] Stations table loads with columns: ID, Name, City, Owner, Coordinates, Operational, is_test
- [ ] Create Station modal has owner dropdown populated from partners API
- [ ] Creating a station with valid coordinates shows marker on map
- [ ] Clicking station row pans map to that station
- [ ] Clicking marker on map highlights corresponding table row
- [ ] Edit station modal pre-fills existing values
- [ ] Delete requires exact `STN-` ID match
- [ ] Soft-deleted stations disappear after page reload

### Chargers (Data → Chargers)

- [ ] Flat `/data/chargers` table shows all chargers across all stations
- [ ] Station filter dropdown filters chargers by selected station
- [ ] Nested view under station detail shows only that station's chargers
- [ ] Status badges display correct colors (green/amber/red/gray)
- [ ] Create Charger modal has connector type dropdown from API
- [ ] New connector type from Settings appears in dropdown without page reload
- [ ] Delete requires exact `CHG-` ID match
- [ ] Hard-deleted chargers disappear immediately

### Connector Types (Settings → Infrastructure Types)

- [ ] List shows all connector types with name and description
- [ ] Create new type → appears in Chargers connector type dropdown
- [ ] Delete unused type succeeds
- [ ] Delete type in use → shows error message, deletion blocked

### App Settings (Settings → App)

- [ ] Three placeholder cards rendered: Branding, Map Tokens, Dropzones
- [ ] No functional action on click (structure only)

### Cross-Cutting

- [ ] All tables use `<ScrollableTable/>` with min-width 800px
- [ ] No horizontal layout breakage at any viewport ≥800px
- [ ] All destructive actions use `<ConfirmDeleteModal/>` with exact ID match
- [ ] Modal forms are used for all create/edit operations
- [ ] No hardcoded hex colors — all styling via Tailwind design tokens
- [ ] Loading states shown during API fetches
- [ ] Error states shown when API calls fail with retry option
- [ ] Empty states shown for zero-result lists
