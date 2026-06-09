# Quickstart: Integration and Hardening

## Prerequisites

- All 4 apps built and running (Sprints 1.1–1.5 complete)
- json-server running: `pnpm mock` (from repo root)

## Full Loop Verification

### Step 1: Admin creates and verifies a partner

1. Open Dashboard: `pnpm dev:dashboard`
2. Navigate to Partners screen
3. Create a new partner with type "business"
4. Verify the partner appears in the table with flags: unverified, not live, active
5. Click Verify — badge turns green
6. Set is_live to true
7. Navigate to Stations → create a station under this partner
8. Navigate to Chargers → create 2 chargers under that station (one available, one maintenance)

### Step 2: Partner manages their data

1. In the dev role switcher, toggle to "Partner View"
2. Select the partner you just created
3. Verify Overview shows correct stat card counts
4. Navigate to My Stations — verify only this partner's station appears
5. Navigate to My Chargers — update one charger's status to "maintenance"

### Step 3: Driver sees the station

1. Open Driver Web: `pnpm dev:web`
2. Reload the map — verify the station appears with correct marker color
3. Open Driver Mobile: `pnpm dev:mobile`
4. Verify the same station appears on mobile

### Step 4: Admin deactivates the partner

1. Back in Dashboard admin view, deactivate the partner
2. Reload both driver apps — verify the station disappears

### Step 5: Admin deletes the partner

1. In Dashboard admin, attempt to delete the partner
2. Verify the UI blocks deletion with a warning about owned stations

## Fix Sweep Verification

### Form Validation

1. In each Dashboard form, submit with empty required fields — verify inline errors appear
2. In the Station form, enter lat=100 and lng=200 — verify field errors appear

### API Offline

1. Stop json-server (`Ctrl+C`)
2. Navigate to every screen in all 4 apps — verify ErrorState with retry button
3. No crashes on any screen
4. Restart json-server, click Retry — verify data loads

### Cross-Browser (Web)

1. Test Dashboard and Driver Web in Chrome, Firefox, and Safari
2. Verify identical behavior

### Cross-Platform (Mobile)

1. Test Driver Mobile on iOS Simulator and Android Emulator
2. Verify identical behavior
3. Deny location permission — verify Tunisia fallback with no crash

## Documentation

```bash
# Created files
docs/guides/onboarding.md        # Step-by-step setup guide
docs/api/mock-api.md             # All resources, fields, filters, limitations
docs/project/phases/mvp-01-status.md  # Sprint completion report
docs/project/decisions.md        # Partner deletion cascade/block decision
```
