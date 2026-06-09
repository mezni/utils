# Quickstart: Driver Web App

**Date**: 2026-06-09

**Prerequisites**:
- Sprint 1.1 complete (json-server running)
- All shared design tokens installed

## Setup

```bash
# Terminal 1: json-server (from repo root)
pnpm mock

# Terminal 2: Driver Web App
pnpm dev:web
```

Open `http://localhost:5173` in a browser.

## Verify Map Screen

### Station Markers
- Map loads centered on Tunisia at zoom 7
- Green markers for stations with available chargers (PRT001 and PRT002 stations)
- Red markers for stations with zero available chargers
- PRT003 (is_verified: false) stations are NOT visible

### Marker Popups
- Click a green marker → popup shows station name, address, "3/5 available", "View Details" link
- Click a red marker → popup shows "0/2 available"

## Verify Station Detail

### Navigation
- Click "View Details" on any marker popup → navigates to `/stations/:id`
- Screen shows station name and address at top
- Charger list shows connector type, power kW, status badge for each charger

### Back Navigation
- Click back button → returns to map at same position and zoom level

## Verify Error Handling

- Stop `pnpm mock` → both screens show error state with Retry
- Restart and click Retry → data loads again

## Verify Dashboard Integration

1. Open Dashboard in another browser tab (`pnpm dev:dashboard`)
2. Switch to Partner View, select PRT001
3. Set a charger status to "maintenance"
4. Refresh the Driver Web App map → affected station marker turns red

## Expected Results

- Only verified/live/active partner stations visible on map
- Green markers = has available chargers, red markers = none available
- Popups show station name, address, charger availability, detail link
- Station Detail shows full charger info
- Error states work on both screens
- Charger status changes in Dashboard reflected on map refresh
