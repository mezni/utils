# MVP-1: Admin Flow — Infrastructure + Auth + Admin Service

> **Scope:** Admin → Traefik → Auth → Admin Service → DB + Redis + Analytics
> **Depends on:** MVP-0 (scaffolding)
> **Status:** Not started

---

## Acceptance criteria

1. Admin/partner operator can log in via Dashboard → Auth Service → Keycloak
2. Auth Service upserts USR- profile into `users` schema on login/refresh
3. Admin Service handles partner CRUD, station/charger management in transactions
4. All inventory writes trigger synchronous Redis cache bust after commit
5. Every mutation is logged to `analytics_db`
6. Traefik validates JWT via cached JWKS before forwarding to Admin Service
7. No service bypasses Traefik. No service touches Keycloak except Auth Service.

---

## Execution order (5 phases)

### Phase 1 — Infrastructure

**Keycloak (`source/infra/keycloak/`)**
- Single realm: `bornemap`
- Clients: `dashboard-app`, `mobile-driver-app`, `admin-dashboard`
- Roles: `role:admin`, `role:partner`, `role:driver`
- Enable OIDC password + refresh flows
- Enable JWKS endpoint: `/realms/bornemap/protocol/openid-connect/certs`

**PostgreSQL (`source/infra/migrations/`)**
- `platform_db` with schemas: `gis`, `inventory`, `users`
- `analytics_db` (isolated, owned by Admin Service)
- PostGIS extension enabled
- Materialized views: `mv_stations_geo`, `mv_stations_summary`, `mv_stations_reviews`

**Redis**
- GIS tile caching keys: `stations:tile:{z}:{x}:{y}`, `stations:near:{lat}:{lng}:{radius}`

**Traefik (`source/infra/traefik/`)**
- Reverse proxy routing:
  - `/api/v1/auth/*` → Auth Service (:3000)
  - `/api/v1/admin/*` → Admin Service (:3002)
  - `/api/v1/driver/*` → Driver Service (:3001)
- JWKS validation middleware (cached, TTL 10-30 min)
- Header injection: `X-User-Id` (sub), `X-User-Roles` (realm_access.roles)

### Phase 2 — Auth Service (`source/services/auth-service/`)

Actix-web service on :3000.

**Endpoints:**
- `POST /api/v1/auth/login` — credentials → Keycloak token call → upsert USR- profile → return JWT + refresh
- `POST /api/v1/auth/refresh` — refresh_token → Keycloak rotate → return new tokens

**Key rules:**
- Sole caller of Keycloak. No other service touches Keycloak APIs.
- Never expose Keycloak endpoints to clients.
- Map Keycloak `sub` claim → USR- row in `users` schema.

**Dependencies:** sqlx (compile-time), reqwest, serde, JWT crate.

### Phase 3 — Admin Service (`source/services/admin-service/`)

Actix-web service on :3002.

**Endpoints:**
- `POST /api/v1/admin/partner` — create partner
- `PUT /api/v1/admin/partner/:id` — update partner
- `POST /api/v1/admin/station` — create station
- `PUT /api/v1/admin/station/:id` — update station
- `POST /api/v1/admin/charger` — create charger
- `PUT /api/v1/admin/charger/:id` — update charger

**Transaction contract (strict):**
```
BEGIN TX
  Write inventory.partners / stations / chargers
COMMIT TX
Redis cache bust (synchronous, after commit)
analytics_db log insert
```

**Key rules:**
- Reads `X-User-Id` and `X-User-Roles` from Traefik headers (never from client body)
- Every mutation logged to `analytics_db` with actor ID, action, timestamp, diff
- Redis invalidation is synchronous per constitution (MVP phase — no event bus)

### Phase 4 — Gateway Security

Wire Traefik JWKS validation middleware against Keycloak certs endpoint.
Validate: signature, `exp`, `aud`, `iss`. Cache public keys with TTL.

### Phase 5 — Dashboard (`source/apps/dashboard/`)

**Auth:** login via Auth Service (never direct to Keycloak). JWT in memory only.
**Role gating:** admin-only routes gated on `role:admin` / `role:partner`.
**Admin actions:** create partner, edit station, manage chargers, view analytics logs (read-only).

---

## Entity ID prefixes (NanoID)

All entity primary keys use NanoID strings with the following prefixes:

| Prefix | Entity | Used in |
|--------|--------|---------|
| `USR-` | User profile | `users` schema (Auth Service only) |
| `OPR-` | Partner/Operator | `inventory.partners` |
| `STA-` | Station | `inventory.stations` |
| `CHG-` | Charger | `inventory.chargers` |

Example: `STA-a1b2c3d4e5f6g7h8i9j0`

CHECK constraints use regex: `id ~ '^(STA|CHG|OPR|USR)-.+'`

---

## Shared types needed

### TypeScript (`source/packages/shared-types/`)
```typescript
interface PartnerDTO { id: string; name: string; /* ... */ }
interface StationDTO { id: string; partnerId: string; location: { lat: number; lng: number }; /* ... */ }
interface ChargerDTO { id: string; stationId: string; status: string; /* ... */ }
```

### Rust (`source/crates/db-models/`, `source/crates/validation/`)
- sqlx typed query models for `inventory.partners`, `inventory.stations`, `inventory.chargers`
- Domain validation rules shared across services

---

## Non-negotiable constraints

- [ ] No service bypasses Traefik
- [ ] No service touches Keycloak except Auth Service
- [ ] All multi-table writes wrapped in explicit `sqlx` transaction
- [ ] Cache bust after `tx.commit()`, never before
- [ ] `X-User-Id` / `X-User-Roles` trusted from Traefik only (never from client)
- [ ] Analytics writes go to isolated `analytics_db` (never `platform_db`)
- [ ] All endpoints under `/api/v1/`
- [ ] JWT never stored in `localStorage` (in-memory only)
- [ ] No `unwrap()` / `expect()` outside test code
- [ ] No raw SQL strings — `sqlx::query!` macros only

## Verification

- `cargo test` — all unit + integration tests pass
- `cargo clippy -- -D warnings` — zero warnings
- Dashboard login flow works end-to-end
- Partner CRUD creates DB rows + busts Redis + logs to analytics
- Traefik returns 401 on expired/malformed JWT
