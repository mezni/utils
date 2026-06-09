# Quickstart: Dashboard Partner View

**Date**: 2026-06-09

**Prerequisites**:
- Sprint 1.1 complete (json-server running)
- Sprint 1.2 complete (Dashboard App running)

## Setup

```bash
# Already running from Sprint 1.2
pnpm mock          # Terminal 1: json-server on :3001
pnpm dev:dashboard # Terminal 2: Dashboard on :5173
```

## Verify Partner Screens

### Set up Partner View
1. Open `http://localhost:5173`
2. Click the "Switch" button at the bottom of the sidebar to toggle to Partner View
3. Select PRT001 from the partner dropdown

### Partner Overview
- Three stat cards: own stations count, own chargers count, available chargers count
- Status bar shows: "Verified ✓" (green), "Live ✓" (green), "Active ✓" (green) for PRT001
- Table of PRT001's stations with name, charger count, availability

### Switch to PRT002
- Select PRT002 from the partner dropdown
- Status bar shows: "Verified ✓" (green), "Not Live" (gray), "Active ✓" (green)
- Different station set

### Switch to PRT003
- Status bar shows: "Awaiting Verification" (gray), "Not Live" (gray), "Active ✓" (green)

### My Stations
- Table shows only PRT001's stations
- Add Station — partner_id is pre-filled and locked to PRT001
- Edit and Delete work on own stations

### My Chargers
- Table shows only chargers belonging to PRT001's stations
- Station filter shows only PRT001's stations
- Add Charger — station select lists only own stations
- Edit and Delete work

### Availability
- Table shows PRT001's stations with current availability status
- Click "Unavailable" on a station → status changes
- Verify by fetching `GET /api/station_availability?station_id=STN001`
- Click "Available" → status reverts

### Error Handling
- Stop `pnpm mock` → all screens show ErrorState with Retry
- Restart and click Retry → data loads again
- No partner selected → prompt to select from dropdown

## Expected Results

- Partner sees only their own data on all 4 screens
- Switching partners shows different scoped data
- Status bar reflects partner's is_verified, is_live, is_active flags
- Availability toggle creates new station_availability records
- Error states work consistently
