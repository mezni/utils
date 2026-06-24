# Sprint 03 — Quickstart

**Prerequisites**: pnpm 9+, Node.js 18+, PostgreSQL 16+, driver-service running

---

## 1. Database Setup

No database changes needed — driver-service reads from existing `gis` schema (from Sprint 01).

Ensure driver-service is running:
```bash
cd source/services/driver-service
export DATABASE_URL="postgres://bornemap:bornemap@localhost:5432/bornemap"
cargo run
```

---

## 2. Build & Run Frontend

```bash
cd source/apps/web-driver

# Option 1: Using .env file
cat > .env <<EOF
VITE_API_BASE_URL=http://localhost:3001
EOF

# Option 2: Using .env.local (preferred)
echo 'VITE_API_BASE_URL=http://localhost:3001' > .env.local

# Install dependencies
pnpm install

# Start dev server
pnpm dev
```

---

## 3. Test Endpoints

### API Tests

```bash
# Health check
curl http://localhost:3001/api/v1/health

# Nearby stations (Tunisia center, 50km)
curl "http://localhost:3001/api/v1/stations/nearby?lat=34.0&lon=9.5&radius=50000&limit=50"

# Error: invalid lat
curl "http://localhost:3001/api/v1/stations/nearby?lat=999&lon=9.5"
```

### UI Tests

```bash
# Run unit tests
pnpm test

# Run typecheck
pnpm typecheck
```

---

## 4. Architecture

```
ui-kit (packages/ui-kit)
  ├── MapProvider.tsx        - Leaflet map wrapper
  ├── StationMarkerLayer.tsx - Clustering markers
  ├── LoadingSpinner.tsx     - Loading state
  ├── ErrorBanner.tsx        - Error state
  └── EmptyState.tsx         - Empty state

domain-types (packages/domain-types)
  ├── StationDto            - Station type
  ├── NearbyResponse        - API response envelope
  └── StationSchema         - Zod validation

client-core (packages/client-core)
  ├── fetchNearbyStations() - API client
  └── useNearbyStations()   - React hook

web-driver (apps/web-driver)
  ├── MapPage.tsx           - Main map page
  ├── useStationsNearViewport.ts - Debounced viewport tracking
  └── stationService.ts     - API wrapper
```

---

## 5. Design System

**Style**: Exaggerated Minimalism (dark theme)
**Colors**:
- Background: #0F172A (slate 900)
- Accent: #2563EB (blue 600)
- Text: #FFFFFF (white)

**Typography**: Inter font family (300–700 weights)

**States**:
- Loading → Spinner overlay
- Success → Map with markers
- Error → Banner with retry
- Empty → "No stations found" message

---

## 6. Map Features

- **Viewport**: Tunisia center (34.0, 9.5, zoom 6)
- **Clustering**: At zoom < 10, markers group
- **Markers**: Station ID, name, distance
- **Interactions**: Hover tooltip, click popup
- **Updates**: Debounced 300ms on map drag
