# Quickstart — BorneMap MVP-1

## Prerequisites

- Docker & Docker Compose
- Make
- Node.js >= 20 (for local frontend dev)
- Expo Go (mobile testing)

## Stack

| Layer | Tech | Port |
|-------|------|------|
| Database | PostGIS 17-3.4 | 5432 |
| API | Rust + Actix-Web 4 | 3001 |
| Gateway | Traefik v3 | 80 |
| Web Client | Vite + React + Leaflet | 5173 |
| Mobile Client | Expo SDK 54 + react-native-maps | — |

## Quick Start

```bash
# 1. Start everything
make up

# 2. Load demo stations
make seed

# 3. Open web app
open http://localhost

# Web at http://localhost/ · API at http://localhost/api/v1/
```

## API Endpoints (via Traefik)

```bash
# Health
curl -s http://localhost/api/v1/health

# Nearby stations (Tunis center) — quote URLs with & params
curl -s 'http://localhost/api/v1/stations/nearby?longitude=10.1815&latitude=36.8065&radius=50000'

# Direct (skip Traefik)
curl -s http://localhost:3001/api/v1/health
```

## Notable Make Targets

| Command | What it does |
|---------|-------------|
| `make up` | Start all containers in background |
| `make down` | Stop all containers |
| `make build` | Rebuild Docker images |
| `make logs` | Tail all container logs |
| `make psql` | Open psql in running postgres container |
| `make import-osm` | Fetch live EV stations from Overpass API |
| `make seed` | Insert 5 demo stations |
| `make clean` | Stop and delete volumes (DB wipe) |

## Manual Import (OSM)

```bash
# Requires: curl, awk, psql (or jq)
make import-osm
```

Source records written to `gis.osm_stations` with `source = 'OSM_IMPORT'`.

## Mobile Dev (Expo)

```bash
cd source/apps/mobile-app
npm install
npx expo start
```

Scan QR with Expo Go. Ensure your phone can reach the API (use host machine IP).

## Web Dev (Vite)

```bash
cd source/apps/web-driver
npm install
npm run dev
```

App at http://localhost:5173

## Architecture

```
Internet → Traefik :80
  ├── /api/v1/* → driver-service :3001 → PostGIS
  └── /          → web-driver :80 (static build)
```

See `docs/architecture.md` for full diagram and `docs/api-contracts.md` for endpoint docs.
