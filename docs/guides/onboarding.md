# Onboarding Guide — BorneMap

## Prerequisites

- Node.js 22+
- pnpm 9+
- Git
- iOS Simulator (macOS with Xcode) or Android Emulator (for Driver Mobile)

## Quick Start

```bash
# Clone the repository
git clone https://github.com/mezni/BorneMap.git
cd BorneMap

# Install dependencies
pnpm install

# Start the mock API
pnpm mock
```

The mock API (json-server) starts on `http://localhost:3001`. All resources are served under the `/api` prefix.

## Available Apps

### 1. Dashboard App (Admin + Partner views)

```bash
pnpm dev:dashboard
```

Opens at `http://localhost:5173`. Use the dev role switcher at the bottom of the sidebar to toggle between Admin View and Partner View.

### 2. Driver Web App

```bash
pnpm dev:web
```

Opens at `http://localhost:5174`. Full-screen Leaflet map with station markers.

### 3. Driver Mobile App

```bash
pnpm dev:mobile
```

Opens Expo dev server. Scan QR code with Expo Go, or press `i` for iOS Simulator / `a` for Android Emulator.

## Verification

1. Open Dashboard at `http://localhost:5173`
2. Create a partner (Partners → Add Partner)
3. Verify the partner, set is_live to true
4. Create a station and chargers under that partner
5. Switch to Partner View → verify scoped data
6. Open Driver Web at `http://localhost:5174` → verify station appears
7. Open Driver Mobile → verify station appears

## Project Structure

```
source/
├── apps/
│   ├── dashboard/        # Vite + React + Tailwind — Admin + Partner Dashboard
│   ├── driver-web/        # Vite + React + Tailwind + Leaflet
│   └── driver-mobile/     # Expo SDK 54 + React Native
├── mock/                  # json-server with db.json
└── packages/
    └── ui/                # Design tokens + Tailwind config base

docs/
├── api/mock-api.md        # Mock API reference
├── guides/onboarding.md   # This guide
└── project/
    ├── phases/            # Sprint completion reports
    └── decisions.md       # Architecture decisions
```

## Commands

| Command | Purpose |
|---------|---------|
| `pnpm mock` | Start json-server on port 3001 |
| `pnpm dev:dashboard` | Start Dashboard app on port 5173 |
| `pnpm dev:web` | Start Driver Web app on port 5174 |
| `pnpm dev:mobile` | Start Driver Mobile app (Expo) |
| `pnpm dev` | List available commands |

## Troubleshooting

- **Port in use**: json-server uses port 3001. If occupied, update the port in `source/mock/package.json`.
- **API not reachable from mobile**: Use the host machine's LAN IP instead of localhost. iOS Simulator can reach localhost; Android Emulator needs `10.0.2.2`.
- **Vite proxy**: Dashboard and Driver Web proxy `/api` to `http://localhost:3001`. No proxy needed for Driver Mobile.
