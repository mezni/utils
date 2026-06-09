# Quickstart: Driver Mobile App

## Prerequisites

- Node.js 22+
- pnpm 9+
- Expo CLI (`pnpm add -g expo-cli` or use `npx expo`)
- iOS Simulator (macOS with Xcode) or Android Emulator
- json-server running on port 3001 (from monorepo root: `pnpm dev:mock`)

## Setup

```bash
# From monorepo root
cd source/apps/driver-mobile
pnpm install
```

## Configuration

Set the API base URL in `src/api/client.ts`:

```typescript
// iOS Simulator → localhost works
// Android Emulator → use host machine LAN IP
// Physical device → use host machine LAN IP
const API_BASE = Platform.select({
  ios: 'http://localhost:3001/api',
  android: 'http://10.0.2.2:3001/api',  // Android Emulator → host loopback
  default: 'http://192.168.x.x:3001/api',
});
```

## Development

```bash
# Start Expo dev server
pnpm dev

# Or with specific platform
pnpm dev:ios
pnpm dev:android
```

## Scripts

| Script | Command | Purpose |
|--------|---------|---------|
| `dev` | `expo start` | Start Expo dev server |
| `dev:ios` | `expo start --ios` | Start and open in iOS Simulator |
| `dev:android` | `expo start --android` | Start and open in Android Emulator |
| `lint` | `eslint src/` | Lint source code |
| `ts:check` | `tsc --noEmit` | TypeScript type check |

## Verification

1. Start json-server: `pnpm dev:mock` (from repo root)
2. Start mobile app: `pnpm dev` (from `source/apps/driver-mobile`)
3. In simulator/emulator:
   - Grant location permission → map centers on device location
   - Deny location permission → map centers on Tunisia
   - Green pins for stations with available chargers
   - Red pins for stations with zero available chargers
   - Tap marker → callout with station name and charger count
   - Tap callout → Station Detail with charger list
   - Tap back → returns to map

## Project Commands (monorepo root)

```bash
pnpm dev:mobile    # Start Driver Mobile App (to be wired)
```
