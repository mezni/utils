# Quickstart: Mobile Driver App — Map Discovery

## Prerequisites

- Expo Go app installed on iOS or Android device
- Backend API deployed and accessible from mobile network
- Expo SDK 51 locked dependencies installed

## Setup

```bash
cd sources/frontend
pnpm install

# Start Expo development server
cd apps/mobile-driver
npx expo start
```

Scan the QR code with Expo Go on your device.

## Backend Change Required

Make `/api/v1/stations/nearby` publicly accessible in `sources/backend/src/domain/infrastructure/mod.rs` — remove the JWT auth requirement from this specific endpoint.

## Validation Checklist

### 1. Map Canvas
- [ ] App opens to full-viewport map centered on Tunisia
- [ ] Map tiles load correctly (Apple Maps on iOS, Google Maps on Android)
- [ ] Map responds to pan and zoom gestures
- [ ] Location permission dialog appears on first launch

### 2. Nearby Discovery
- [ ] Granting location re-centers map on current position
- [ ] Station markers (green circle with bolt icon) appear within 20km
- [ ] Denying location shows explanatory message, map stays on Tunisia
- [ ] Panning to area with no stations shows empty state
- [ ] More than 20 results are clustered appropriately

### 3. Station Detail Sheet
- [ ] Tapping a marker opens bottom sheet with station name, address, distance, charger count
- [ ] Charger list shows connector type, power, current type, status badge
- [ ] Swiping down dismisses the sheet
- [ ] Tapping a different marker updates the sheet content

### 4. Navigation
- [ ] "Navigate" button in sheet opens device maps app
- [ ] Device maps app shows station coordinates as destination

### 5. Radius & Refresh
- [ ] Changing radius from 20km to 50km shows additional markers
- [ ] Changing radius to 5km removes distant markers
- [ ] Pull-to-refresh re-fetches stations

### 6. Error Handling
- [ ] No internet connection shows error message (not crash)
- [ ] API error shows retry option (not crash)
- [ ] Location permission denied shows clear explanation
- [ ] App never shows blank white screen

## Test Commands

```bash
# Type-check
cd sources/frontend && pnpm -r type-check

# Lint
cd sources/frontend && pnpm -r lint

# Build (checks for Expo build issues)
cd sources/frontend && pnpm -r build

# Start Expo (physical device testing)
cd sources/frontend/apps/mobile-driver && npx expo start
```
