# @bornemap/web-driver

React-based web driver app for BorneMap. Built with React 18, Vite, Leaflet, and TanStack Query.

## Setup

```bash
pnpm install
```

## Development

```bash
pnpm dev
```

The app runs on `http://localhost:5173` by default.

## Build

```bash
pnpm build
pnpm preview  # Preview production build
```

## Structure

```
src/
  pages/         - Route pages (index.tsx map, stations.tsx list, station/[id].tsx detail)
  components/    - Reusable UI components
  services/      - API services and data fetching
  store/         - Zustand stores (theme)
  hooks/         - Custom React hooks (useLeafletMap)
  utils/         - Utility functions (offline cache, map clustering)
  theme/         - Theme provider
```

## Features

- Map-based station discovery (Leaflet + OpenStreetMap)
- Station list with search and pagination
- Station detail with charger information
- Dark mode with localStorage persistence
- Offline caching (last 50 stations)
- Error handling with retry
- Responsive design (mobile, tablet, desktop)

## API Integration

The app expects an API at `/api/stations`. For development, configure in `.env`.

## Configuration

Copy `.env.example` to `.env` and adjust.

## Testing

```bash
pnpm lint
pnpm typecheck
```
