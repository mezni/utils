# Product Backlog

Prioritized list of features and work, organized by MVP. See `implementation-plan.md` for sprint breakdowns.

## MVP-1 Backlog

### Epic: Core Product Loop

**Goal**: Partner creates stations, driver finds nearby stations on map, full loop works end-to-end.

#### Backend and Database (Sprint 1.1)
- [ ] Initialize FastAPI project under `source/services/bornemap-service/`
- [ ] Set up PostgreSQL database `ev_platform` locally
- [ ] Create `inventory` and `gis` schemas
- [ ] Implement partner, station, charger tables
- [ ] Implement 16 CRUD endpoints with Pydantic models
- [ ] Write Alembic migrations
- [ ] Seed database with 3 partners, 15 stations, 24 chargers
- [ ] Write smoke tests for all endpoints
- [ ] Verify `GET /api/health` works

#### Dashboard App (Sprint 1.2)
- [ ] Initialize Vite + React project at `source/apps/dashboard/`
- [ ] Create design token base config in `source/packages/ui/`
- [ ] Implement AppShell with sidebar navigation
- [ ] Build Overview screen with StatCards
- [ ] Build Partners screen with CRUD
- [ ] Build Stations screen with CRUD and filters
- [ ] Build Chargers screen with CRUD and status updates
- [ ] Add form validation and error states
- [ ] Test on Chrome, Firefox, Safari

#### Driver Web App (Sprint 1.3)
- [ ] Initialize Vite + React project at `source/apps/driver-web/`
- [ ] Set up Leaflet + react-leaflet
- [ ] Build full-bleed map with OpenStreetMap tiles
- [ ] Fetch and render station markers with color logic
- [ ] Build Station Detail screen
- [ ] Add floating UI components (top bar, zoom controls)
- [ ] Test on Chrome, Firefox, Safari

#### Driver Mobile App (Sprint 1.4)
- [ ] Initialize Expo project at `source/apps/driver-mobile/`
- [ ] Set up react-native-maps and expo-location
- [ ] Build map screen with permission handling
- [ ] Fetch and render station markers
- [ ] Build Station Detail screen
- [ ] Test on iOS simulator
- [ ] Test on Android emulator

#### Integration and Hardening (Sprint 1.5)
- [ ] Verify full loop: Dashboard → Driver Web → Driver Mobile
- [ ] Run all smoke tests
- [ ] Fix any visual regressions
- [ ] Check performance (endpoints under 200ms)
- [ ] Write onboarding guide
- [ ] Write API documentation
- [ ] Write phase status file

---

## MVP-2 Backlog

Planned for after MVP-1 is complete and validated.

### Epic: Production Services + Spatial Queries

- [ ] Initialize Cargo workspace
- [ ] Build `ev-core` crate with NanoID generation
- [ ] Build `ev-db` crate with pool management
- [ ] Migrate database to PostGIS
- [ ] Migrate UUIDs to NanoID prefixed identifiers
- [ ] Implement Driver Service in Rust
- [ ] Implement Admin Service in Rust
- [ ] Write Docker Compose stack
- [ ] Set up GitHub Actions CI/CD
- [ ] Test full loop with Rust services
- [ ] Update frontend base URLs

---

## MVP-3 Backlog

Planned for after MVP-2.

### Epic: Authentication + User Management

- [ ] Set up Keycloak in Docker Compose
- [ ] Create `users` schema
- [ ] Implement `ev-auth` shared crate
- [ ] Add JWT middleware to both services
- [ ] First-login provisioning in Driver Service
- [ ] Implement auth flow in Driver Web App
- [ ] Implement auth flow in Driver Mobile App
- [ ] Add role-based navigation to Dashboard
- [ ] Introduce Arabic and French i18n
- [ ] Implement RTL layout in all apps

---

## MVP-4, MVP-5, MVP-6 Backlogs

See `implementation-plan.md` for full roadmap.

---

## Deferred Features

See `out-of-scope-registry.md` for permanently deferred work.
