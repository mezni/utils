# Data Model: MVP-1 Infrastructure

## Service Layout

```
services/auth-service/        port 3000
  ├── Cargo.toml
  ├── src/
  │   ├── main.rs
  │   ├── routes/
  │   │   ├── health.rs       GET /api/v1/health
  │   │   └── ready.rs        GET /api/v1/health/ready
  │   ├── config.rs           HOST, PORT, DATABASE_URL, LOG_LEVEL
  │   └── db.rs               Connection pool to platform_db

services/driver-service/      port 3001
  └── (same structure)

services/admin-service/       port 3002
  └── (same structure)
```

## App Layout

```
apps/mobile-driver/           Expo SDK 54
  ├── app.json
  ├── App.tsx                 MapView centered on Tunisia
  ├── package.json
  └── tsconfig.json

apps/web-driver/              React + Leaflet
  ├── src/
  │   ├── App.tsx             MapView centered on Tunisia
  │   └── main.tsx
  ├── package.json
  └── vite.config.ts

apps/dashboard/               React + shadcn/ui
  ├── src/
  │   ├── App.tsx             Router shell
  │   ├── pages/
  │   │   └── Login.tsx       Branded logged-out state
  │   └── main.tsx
  ├── package.json
  └── vite.config.ts
```

## Package Layout

```
packages/shared-types/        TypeScript type definitions
packages/shared-ui/           Reusable UI components (base layout)
packages/shared-hooks/        Shared React hooks
packages/api-client/          Typed API client

crates/db-models/             Rust DB model structs + enums
crates/validation/            Rust validation logic
```

## Database Schemas (platform_db)

```
gis/     — OSM reference data    (empty until MVP-2 importer runs)
inventory/ — partner, station, charger tables  (empty until MVP-4)
users/     — driver_profile, driver_favorite   (empty until MVP-3)
```

Each schema created via `infra/db/init-platform-db.sql` on first container start.
