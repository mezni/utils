# Quickstart: Mobile Driver App (Core UX)

**Phase**: Phase 1 | **Date**: 2026-06-11 | **Feature**: [spec.md](spec.md)

## Prerequisites

- Node.js 20+
- Expo SDK 54 CLI (`npx expo`)
- iOS Simulator (macOS) or Android Emulator
- Driver Service running at `http://localhost:8080`
- Clickstream Service running at `http://localhost:8082`

## Setup

### 1. Initialize the Expo app

```bash
# From source/front/
npx create-expo-app@latest . --template blank-typescript
```

### 2. Install project dependencies

```bash
npx expo install react-native-maps expo-location expo-router react-native-safe-area-context react-native-screens
```

### 3. Configure Expo Router

Ensure `app.json` has:

```json
{
  "scheme": "borne",
  "plugins": ["expo-router"]
}
```

### 4. Link the design system package

```bash
npm install @borne/design-system@file:./packages/design-system
```

### 5. Configure service URLs

Create `src/services/config.ts`:

```ts
export const config = {
  driverServiceUrl: process.env.EXPO_PUBLIC_DRIVER_URL ?? 'http://localhost:8080',
  clickstreamUrl: process.env.EXPO_PUBLIC_CLICKSTREAM_URL ?? 'http://localhost:8082',
};
```

## Development

```bash
# Start Expo dev server
npx expo start

# Run tests
npx jest
```

## Verification Checklist

1. [ ] App launches to full-screen map centered on Tunis (or GPS location)
2. [ ] Station markers appear on the map
3. [ ] Panning the map re-fetches nearby stations
4. [ ] Tapping a marker opens the bottom sheet with station name and distance
5. [ ] Bottom sheet shows charger list with connector types and statuses
6. [ ] Map skeleton shows during initial load
7. [ ] Sheet skeleton shows during detail fetch
8. [ ] Error state with "Retry" appears when Driver Service is down
9. [ ] Empty state appears when no stations are nearby
10. [ ] Events (map_open, station_click, station_view) appear in analytics_db

## Project Structure

```
source/front/
├── app/
│   ├── _layout.tsx
│   └── index.tsx
├── src/
│   ├── components/
│   │   ├── MapScreen.tsx
│   │   ├── StationMarker.tsx
│   │   ├── StationBottomSheet.tsx
│   │   ├── ChargerList.tsx
│   │   └── MapErrorState.tsx
│   ├── hooks/
│   │   ├── useNearbyStations.ts
│   │   ├── useStationDetail.ts
│   │   ├── useLocation.ts
│   │   └── useClickstream.ts
│   ├── services/
│   │   ├── api.ts
│   │   └── config.ts
│   └── types/
│       ├── station.ts
│       └── events.ts
├── packages/
│   └── design-system/
├── app.json
├── package.json
└── tsconfig.json
```
