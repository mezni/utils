# Quickstart — Frontend Apps Scaffold

> Set up and run all three frontend apps for development.

## Prerequisites

- Node.js >= 20.20.0
- npm >= 10.8.0
- Expo CLI (`npm install -g expo-cli`)
- iOS Simulator (macOS only) or Android Emulator
- Backend services running (driver-service on :3001, admin-service on :3002)

## Install Dependencies

```bash
# From repo root — installs all workspace packages
npm install
```

## Driver Web

```bash
# Start dev server with Vite proxy to driver-service
npm run dev:driver-web
# Opens at http://localhost:5173
```

The Vite proxy forwards `/api/v1` requests to `http://localhost:3001` (driver-service).

**Key files**:
- `apps/driver-web/src/App.tsx` — Root component with map
- `apps/driver-web/src/components/StationMap.tsx` — Leaflet map
- `apps/driver-web/src/hooks/useStations.ts` — Station data fetching

## Driver Mobile

```bash
# Start Expo dev server
npm run dev:mobile
# Scan QR code with Expo Go, or press i (iOS) / a (Android)
```

**Key files**:
- `apps/driver-mobile/app/index.tsx` — Map screen (expo-router)
- `apps/driver-mobile/hooks/useLocation.ts` — Location permission + fallback
- `apps/driver-mobile/services/api.ts` — Driver service API client

## Dashboard

```bash
# Start dev server with Vite proxy to admin-service
npm run dev:dashboard
# Opens at http://localhost:5174
```

**Key files**:
- `apps/dashboard/src/components/AppShell.tsx` — Layout shell
- `apps/dashboard/src/components/Sidebar.tsx` — Left navigation
- `apps/dashboard/src/pages/OverviewPage.tsx` — Overview with stat cards

## Build All Web Apps

```bash
npm run build:driver-web
npm run build:dashboard
```

## Verify CI

```bash
# Lint web apps
npm run lint

# Test web apps
npm run test
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Map shows blank tiles | Ensure driver-service is running on :3001 |
| Mobile Expo build fails | Run `npx expo install --fix` in apps/driver-mobile |
| CORS errors (mobile) | Ensure driver-service allows CORS or use 10.0.2.2 for Android emulator |
| Dashboard sidebar broken | Check react-router-dom routes match NavLink `to` paths |
