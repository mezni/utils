# Quickstart: BorneMap Platform Scaffold

## Prerequisites

- Docker & Docker Compose
- Rust toolchain (rustup, cargo)
- Node.js 18+
- Expo Go app on iOS/Android device (or emulator)

## 1. Start Local Database

```bash
docker compose up -d
```

Starts PostGIS 15 on port 5432. Health check runs every 5 seconds — wait for `(healthy)`.

## 2. Configure Environment

```bash
cp .env.example .env
# Edit DATABASE_URL, JWT_SECRET, and EXPO_PUBLIC_API_URL if needed
```

## 3. Run Backend API

```bash
cd backend && cargo run -p api-service
```

API available at `http://0.0.0.0:8080/api/v1/stations/nearby`.

## 4. Verify API

```bash
curl http://127.0.0.1:8080/api/v1/stations/nearby?lat=36.8065\&lng=10.1815
```

Expected response: JSON array of StationHub objects with nanouuid IDs.

## 5. Launch Mobile App

```bash
cd apps/mobile-driver
npm install
npx expo start
```

Scan QR code with Expo Go app. Map should load centered on Tunis with station markers.

## Available Make Commands

```bash
make up          # Start PostGIS database
make down        # Stop database
make status      # Check container status
make test-backend # Run cargo test --workspace
make dev-api     # Run api-service
```

## CI Pipeline

Every push/PR to `main` or `develop` triggers:
1. Rust format check (`cargo fmt --check`)
2. Compilation check (`cargo check --workspace`)
3. Unit tests (`cargo test --workspace`)
4. Expo web export (verifies mobile build)

## Project Layout

```
borne-map/               # Repository root
├── .github/workflows/   # CI pipeline
├── apps/                # Mobile + admin clients
├── backend/             # Rust multi-crate workspace
├── db/                  # Migrations + seed data
├── deployments/         # Production Docker Compose + nginx
├── docs/                # Architecture docs + runbooks
├── .env.example         # Environment template
├── docker-compose.yml   # Local dev stack
└── Makefile             # Dev workflow shortcuts
```
