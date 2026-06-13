# @bornemap/mobile-driver

Expo-based mobile driver app for BorneMap. Built with Expo SDK 50, React Native 0.73, and Expo Router v3.

## Setup

```bash
pnpm install
```

## Development

```bash
pnpm dev
# or
pnpm start
```

The app uses Expo Router file-based routing. Screens are in `app/`.

## Build

```bash
# Export Android bundle
npx expo export --platform android --output-dir dist

# For EAS builds (requires Expo account)
eas build --platform android
eas build --platform ios
```

## Structure

```
app/            - Expo Router screens (_layout.tsx, index.tsx, stations.tsx, station/[id].tsx)
components/     - Reusable UI components
services/       - API services, caching, geolocation, notifications
store/          - Zustand stores (theme, station, map)
theme/          - Theme provider
utils/          - Utility functions (map clustering)
hooks/          - Custom React hooks (empty - hooks in components)
```

## Features

- Map-based station discovery (react-native-maps)
- Station list with search and pagination
- Station detail with charger information
- Dark mode with persistent theme
- Offline caching (last 50 stations)
- Pull-to-refresh
- Haptic feedback
- Error handling with retry and copy-to-clipboard

## API Integration

The app expects an API at `http://localhost:8080` by default. Configure via environment.

## Configuration

Copy `.env.example` to `.env` and adjust:

```
API_BASE_URL=http://localhost:8080
OSM_NOMINATIM_URL=https://nominatim.openstreetmap.org
```

## Testing

```bash
pnpm test
pnpm lint
pnpm typecheck
```

### E2E Testing (Maestro)

E2E tests use [Maestro](https://maestro.mobile.dev) for mobile flows:

```bash
# Install Maestro CLI
curl -Ls "https://get.maestro.mobile.dev" | bash

# Run E2E tests (requires running backend + Android emulator / iOS simulator)
maestro test e2e/discovery-flow.yaml
```

**Prerequisites**: Android emulator or iOS simulator must be running.
**Fallback**: Web-only E2E tests (Playwright) do not require an emulator. See `specs/005-integration-testing/quickstart.md`.
