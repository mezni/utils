# Quickstart: Mobile Driver App (Expo SDK 54)

## Prerequisites

- Node.js 20+ with `npm` or `yarn`
- Expo CLI: `npx expo --version` (should output 54.x)
- Expo Go app installed on your iOS/Android device
- Docker Compose running with `platform_db` + `driver-service` + `traefik` (from Sprint 1.1/1.2)
- Developer machine and mobile device on the same LAN

## 1. Environment Setup

```bash
cd source/apps/mobile-driver
cp .env.template .env
```

**`.env` contents:**

```env
# IP of the machine running Docker Compose (Traefik on :80)
# Find yours with: ip addr show | grep 'inet 192'
API_BASE_URL=http://192.168.1.42:80
```

If you don't have a `.env.template`, configure the URL directly in `app.json` under `expo.extra.apiBaseUrl`.

## 2. Install Dependencies

```bash
npm install
```

## 3. Start the Expo Dev Server

```bash
npx expo start
```

This starts the Metro bundler. A QR code appears in the terminal.

## 4. Run on Device

1. Open **Expo Go** on your mobile device
2. **Scan the QR code** from the terminal (iOS: Camera app; Android: Expo Go app)
3. The app loads and requests location permission
4. Grant permission → map centers on your location, fetches nearby stations

> If you're behind a VPN or firewall, use the `--tunnel` flag: `npx expo start --tunnel`

## 5. Test Scenarios

```text
1. Normal flow:  Open app → grant location → see map with station markers
2. Deny location: Open app → deny location → map defaults to Tunis (36.8, 10.18)
3. Loading:      Slow network → shimmer skeleton appears over map
4. Error:        Turn on airplane mode → ErrorBoundary with "Retry Connection"
5. Empty:        Pan to remote Tunisian desert → "No stations nearby" message
6. Offline cache: Load stations → airplane mode → cached markers + banner
7. Macro-zoom:   Pinch zoom out past level 8 → overlay "Zoom in closer"
8. Pull-refresh: Pull down on map → stations re-fetch from API
```

## 6. API Base URL Configuration

### Via `app.json` (recommended for dev)

```json
{
  "expo": {
    "extra": {
      "apiBaseUrl": "http://192.168.1.42:80"
    }
  }
}
```

### Via environment variable (for CI/testing)

```bash
export API_BASE_URL=http://192.168.1.42:80
npx expo start
```

The app reads `expo-constants` extras and falls back to `http://localhost:3001` (emulator).

## 7. Run Tests

```bash
# Unit tests (jest)
npx jest

# Lint
npx eslint src/
```

## 8. Verify Backend Connectivity

Before blaming the mobile app, confirm the backend is reachable:

```bash
# From your mobile device's browser:
# http://<LAN_IP>:80/api/v1/nearby?lat=36.8&lng=10.18&radius=10000

# Or from the developer machine:
curl "http://localhost:80/api/v1/nearby?lat=36.8&lng=10.18&radius=10000"
```

Expected response: `{"stations":[...]}` with 4 stations near Tunis.

## Project Structure

```
source/apps/mobile-driver/
├── App.tsx
├── src/
│   ├── components/       # MapContainer, StationCallout, ShimmerSkeleton,
│   │                     # ErrorBoundary, EmptyState, OfflineBanner, MacroZoomOverlay
│   ├── hooks/            # useDebounce, useNearbyStations
│   ├── services/         # api.ts
│   ├── cache/            # asyncStorage.ts
│   ├── types/            # Station, Viewport, FetchState
│   └── utils/            # coordinates.ts, network.ts
├── assets/markers/       # Charging pin icons
└── package.json
```
