# Constitution

## Product
BorneMap is an EV station discovery and management platform for Tunisia.

## User Types
- Public Driver: browses stations without login
- Registered Driver: authenticated driver with favorites and reviews
- Partner: manages own stations and chargers via dashboard
- Admin: manages the entire platform globally

## Current Scope
- Public station discovery: map, nearby, search, detail
- Partner and admin management: stations, chargers, partners
- Manual availability updates
- GIS synchronization via PostgreSQL triggers in a future MVP
- Clickstream analytics in a future MVP

## Explicit Out of Scope For Current MVPs
- OCPP and charging sessions
- Payments and billing
- Routing and navigation
- Real-time availability
- Push notifications

## MVP Strategy
- Each MVP must be a complete, deployable, usable slice of the platform
- Later MVPs may add to or replace a specific layer, but must not break earlier MVPs
- Build the minimum that proves the core loop works
- Validate before adding complexity
- Never introduce infrastructure the current MVP does not need

## MVP Progression
- MVP-1: prove the product loop with the fastest possible stack
- MVP-2: replace Python service with Rust, add PostGIS, add CI/CD
- MVP-3: add authentication and user management
- MVP-4: add GIS synchronization
- MVP-5: add analytics and reporting
- MVP-6: production hardening, Traefik, launch readiness

## Permanent Architecture Rules

### Data
- `inventory.station` is the source of truth for stations
- `gis` is never the master of any business entity
- analytics lives in `analytics` only
- no additional schemas without an approved ADR

### API
- all endpoints live under `/api`
- every service exposes `GET /api/health` with a database check
- all SQL uses bind parameters
- public API IDs are never sequential integers

### Infrastructure
- only Traefik exposes public ports, introduced in MVP-6
- Keycloak owns authentication, introduced in MVP-3
- no service issues its own tokens
- secrets never live in committed files or container images
- no image registry, images are built on host

### Frontend
- no hardcoded visual values, tokens only
- tokens never live in localStorage or AsyncStorage
- Arabic RTL must be correct on every screen from MVP-3
- public browsing never triggers an auth prompt
- Expo SDK stays on 54 unless an ADR approves change
- map tiles use OpenStreetMap unless an ADR approves otherwise
- analytics errors never surface to the user

## Tooling Roles
- SpecKit: implementation code only
- Impeccable: UX and UI design only
- This assistant: planning, architecture, documentation, tracking
