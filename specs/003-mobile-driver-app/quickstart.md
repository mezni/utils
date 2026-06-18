# Quickstart: Web Driver Client (React + Vite)

**Branch**: `004-web-driver-client` | **Date**: 2026-06-18 | **Spec**: [`spec.md`](../004-web-driver-client/spec.md)

## Prerequisites

- Node.js 20+ with `npm` or `yarn`
- Docker Compose running with `platform_db` + `driver-service` (from Sprint 1.1/1.2)
- Modern web browser (Chrome 90+, Edge 90+, Safari 15+, Firefox 88+)

## 1. Environment Setup

```bash
cd source/apps/web-driver
cp .env.template .env
```

**`.env` contents:**

```env
VITE_API_BASE_URL=http://localhost:3001
```

If you don't have a `.env.template`, configure the URL directly in `src/main.tsx`.

## 2. Install Dependencies

```bash
npm install
```

## 3. Start the Vite Dev Server

```bash
npm run dev
```

This starts the Vite development server. Open `http://localhost:5173` in your browser.

## 4. Run on Production Build

```bash
npm run build
npm run preview
```

Or deploy the `dist/` directory to any static hosting service.

## 5. Test Scenarios

```text
1. Normal flow:  Open app → grant location → see map with station markers
2. Deny location: Open app → deny location → map defaults to Tunis (36.8, 10.18)
3. Loading:      Slow network → shimmer skeleton appears over map
4. Error:        Turn on airplane mode → ErrorBoundary with "Retry Connection"
5. Empty:        Pan to remote Tunisian desert → "No stations nearby" message
6. Offline cache: Load stations → airplane mode → cached markers + banner
7. Zoom out:     Scroll down past zoom level 4 → overlay "Zoom in closer"
8. Manual refresh: Click refresh button → stations re-fetch from API
```

## 6. API Base URL Configuration

### Via environment variable (recommended for dev)

```env
VITE_API_BASE_URL=http://localhost:3001
```

### Via runtime config (for testing)

```typescript
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:3001';
```

### Via browser config (for production)

```typescript
const API_BASE_URL = 'https://api.bornemap.com'; // or set in environment
```

## 7. Run Tests

```bash
# Unit tests (jest)
npx jest

# Lint
npx eslint src/

# Type checking
npx tsc --noEmit
```

## 8. Verify Backend Connectivity

Before blaming the web client, confirm the backend is reachable:

```bash
curl "http://localhost:3001/api/v1/nearby?lat=36.8&lng=10.18&radius=10000"
```

Expected response: `{"stations":[...]}` with 4 stations near Tunis.

## Project Structure

```
source/apps/web-driver/
├── index.html           # Entry point with Leaflet loading
├── src/
│   ├── components/      # MapContainer, StationMarker, ShimmerSkeleton,
│   │                    # ErrorBoundary, EmptyState, OfflineBanner, ZoomOutOverlay
│   ├── hooks/           # useDebounce, useNearbyStations
│   ├── services/        # api.ts
│   ├── cache/           # localStorage.ts
│   ├── types/           # Station, Viewport, FetchState
│   └── utils/           # coordinates.ts, network.ts
├── assets/markers/      # Charging pin SVG/PNG icons for Leaflet
├── public/              # Leaflet CSS/JS and static assets
└── package.json
```
