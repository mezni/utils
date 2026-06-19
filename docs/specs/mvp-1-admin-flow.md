# MVP-1: Admin Flow — Infrastructure + Auth + Admin Service

> **Scope:** Admin → Traefik → Auth → Admin Service → DB + Redis + Analytics
> **Depends on:** MVP-0 (scaffolding)
> **Status:** Not started

---

## Acceptance criteria

1. Admin/partner operator can log in via Dashboard → Auth Service → Keycloak
2. Auth Service upserts USR- profile into `users` schema on login/refresh
3. Admin Service handles partner CRUD, station/charger management in transactions
4. All inventory writes trigger synchronous Redis cache bust after commit (in service layer)
5. Every mutation is logged to `analytics_db` with BEFORE/AFTER diff
6. Traefik validates JWT via cached JWKS before forwarding, including `aud` check
7. No service bypasses Traefik. No service touches Keycloak except Auth Service.
8. PostgreSQL roles per service enforce schema-level access (not just logical)
9. All POST endpoints support `Idempotency-Key` header to prevent duplicate creation
10. Keycloak `/realms/bornemap/protocol/openid-connect/*` blocked from external access

---

## Execution order (5 phases)

### Phase 1 — Infrastructure

**Keycloak (`source/infra/keycloak/`)**
- Single realm: `bornemap`
- Clients: `mobile-driver-app`, `web-driver-app`, `admin-dashboard`
- Roles: `role:admin`, `role:partner`, `role:driver`
- Enable OIDC password + refresh flows
- Enable JWKS endpoint: `/realms/bornemap/protocol/openid-connect/certs`

**PostgreSQL — schema DDL (`source/infra/migrations/0001_init_schemas.sql`)**

```sql
-- Schema setup
CREATE SCHEMA IF NOT EXISTS gis;
CREATE SCHEMA IF NOT EXISTS inventory;
CREATE SCHEMA IF NOT EXISTS users;

-- DB roles per service (physical enforcement of schema ownership)
CREATE ROLE auth_service_role WITH LOGIN;
GRANT USAGE ON SCHEMA users TO auth_service_role;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA users TO auth_service_role;

CREATE ROLE admin_service_role WITH LOGIN;
GRANT USAGE ON SCHEMA inventory TO admin_service_role;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA inventory TO admin_service_role;
-- analytics_db roles applied at that DB level (separate database)

CREATE ROLE driver_service_role WITH LOGIN;
GRANT USAGE ON SCHEMA inventory TO driver_service_role;
GRANT SELECT ON ALL TABLES IN SCHEMA inventory TO driver_service_role;

-- updated_at trigger (required on every table with this column)
CREATE OR REPLACE FUNCTION trigger_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Lookup tables (reference data, SERIAL PKs)
CREATE TABLE inventory.connector_types (
    id   SMALLSERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE inventory.connector_statuses (
    id   SMALLSERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE inventory.current_types (
    id   SMALLSERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE
);

-- Partners (top-level grouping — no networks layer per constitution)
CREATE TABLE inventory.partners (
    id             TEXT        PRIMARY KEY CHECK (id ~ '^OPR-.+'),
    name           VARCHAR(255) NOT NULL,
    network_type   VARCHAR(20) NOT NULL CHECK (network_type IN ('INDIVIDUAL', 'COMPANY')),
    support_phone  VARCHAR(50),
    support_email  VARCHAR(255),
    is_verified    BOOLEAN     DEFAULT FALSE,
    created_by     TEXT,
    updated_by     TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at     TIMESTAMPTZ
);
CREATE INDEX idx_partners_active ON inventory.partners (name) WHERE deleted_at IS NULL;

CREATE TRIGGER set_updated_at
    BEFORE UPDATE ON inventory.partners
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

-- Stations
CREATE TABLE inventory.stations (
    id          TEXT        PRIMARY KEY CHECK (id ~ '^STA-.+'),
    partner_id  TEXT        NOT NULL REFERENCES inventory.partners(id),
    osm_id      BIGINT,                                           -- nullable: partner-created stations have no OSM ref
    name        VARCHAR(255) NOT NULL,
    address     TEXT,
    location    GEOGRAPHY(Point, 4326) NOT NULL,
    tags        HSTORE,
    created_by  TEXT,
    updated_by  TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at  TIMESTAMPTZ
);
CREATE INDEX idx_stations_partner ON inventory.stations (partner_id) WHERE deleted_at IS NULL;
CREATE INDEX idx_stations_location ON inventory.stations USING GIST (location);

CREATE TRIGGER set_updated_at
    BEFORE UPDATE ON inventory.stations
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

-- Chargers
CREATE TABLE inventory.chargers (
    id                TEXT        PRIMARY KEY CHECK (id ~ '^CHG-.+'),
    station_id        TEXT        NOT NULL REFERENCES inventory.stations(id) ON DELETE CASCADE,
    connector_type_id SMALLINT   NOT NULL REFERENCES inventory.connector_types(id),
    status_id         SMALLINT   NOT NULL REFERENCES inventory.connector_statuses(id),
    current_type_id   SMALLINT   NOT NULL REFERENCES inventory.current_types(id),
    power_kw          DECIMAL(5,2),
    voltage           INT,
    amperage          INT,
    count_available   INT        DEFAULT 1 CHECK (count_available >= 0),
    count_total       INT        DEFAULT 1 CHECK (count_total >= 1 AND count_total >= count_available),
    created_by        TEXT,
    updated_by        TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at        TIMESTAMPTZ,
    CONSTRAINT unique_connector UNIQUE (station_id, connector_type_id, current_type_id)
);
CREATE INDEX idx_chargers_station ON inventory.chargers (station_id) WHERE deleted_at IS NULL;

CREATE TRIGGER set_updated_at
    BEFORE UPDATE ON inventory.chargers
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();
```

**`analytics_db`** (separate database, not schema)

```sql
-- Audit log for all Admin Service mutations
CREATE TABLE audit_log (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    actor_id    TEXT        NOT NULL,           -- X-User-Id from Traefik
    action      TEXT        NOT NULL,           -- e.g. 'partner.created', 'station.updated'
    target_type TEXT        NOT NULL,           -- e.g. 'partner', 'station', 'charger'
    target_id   TEXT        NOT NULL,           -- NanoID of the affected entity
    before_snapshot JSONB,                      -- NULL on CREATE, full row on UPDATE
    after_snapshot  JSONB,                      -- full row after mutation
    payload     JSONB,                          -- additional context (request body, metadata)
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_audit_actor ON audit_log (actor_id);
CREATE INDEX idx_audit_target ON audit_log (target_type, target_id);
CREATE INDEX idx_audit_created ON audit_log (created_at DESC);

-- DB role for Admin Service on analytics_db
-- (run on analytics_db, not platform_db)
CREATE ROLE admin_analytics_role WITH LOGIN;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO admin_analytics_role;
```

Written exclusively by Admin Service.

**Materialized views** (created after initial data load, `source/infra/migrations/0002_materialized_views.sql`):
- `inventory.mv_stations_geo` — spatial summary for map display
- `inventory.mv_stations_summary` — list view data
- `inventory.mv_stations_reviews` — aggregated review stats

**Redis**
- GIS tile caching keys: `stations:tile:{z}:{x}:{y}`, `stations:near:{lat}:{lng}:{radius}`
- Key namespace ownership: Admin Service invalidates (`del`), Driver Service reads (`get`)
- Invalidation occurs in the **service orchestration layer**, not the repository layer

**Traefik (`source/infra/traefik/`)**
- Reverse proxy routing:
  - `/api/v1/auth/*` → Auth Service (:3000)
  - `/api/v1/admin/*` → Admin Service (:3002)
  - `/api/v1/driver/*` → Driver Service (:3001)
- JWKS validation middleware (cached, TTL 10-30 min)
- Header injection: `X-User-Id` (sub), `X-User-Roles` (realm_access.roles)
- **Audience validation**: JWT `aud` must match the calling client (`admin-dashboard` for admin routes)
- **Network isolation**: Block external access to `/realms/bornemap/protocol/openid-connect/*` — only Auth Service may reach Keycloak

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

Redis invalidation occurs in the **service orchestration layer** after commit and before HTTP response:

```
BEGIN TX
  Write inventory.partners / stations / chargers
COMMIT TX
Redis cache bust (synchronous, in service layer after commit, before response)
analytics_db log insert with BEFORE/AFTER diff
```

**Idempotency:**
- All `POST` endpoints MUST support `Idempotency-Key` header
- If a request with a previously seen key arrives within 24h, return the original response (201) without re-executing
- Duplicate without key → 409 Conflict
- Key format: UUID v4

**Key rules:**
- Reads `X-User-Id` and `X-User-Roles` from Traefik headers (never from client body)
- Every mutation logged to `analytics_db.audit_log` with:
  - `before_snapshot`: JSON snapshot before mutation (NULL on CREATE)
  - `after_snapshot`: JSON snapshot after mutation
  - `actor_id`: from `X-User-Id`
- Redis invalidation is synchronous per constitution (MVP phase — no event bus)

### Phase 4 — Gateway Security

Wire Traefik JWKS validation middleware against Keycloak certs endpoint.
Validate: signature, `exp`, `aud`, `iss`. Cache public keys with TTL.

**Audience enforcement:**
- Admin routes require JWT `aud` == `admin-dashboard`
- Auth routes may accept multiple audiences
- Reject tokens with mismatched audience at the gateway before forwarding to any service

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
- [ ] Keycloak token endpoints blocked from external network access
- [ ] All multi-table writes wrapped in explicit `sqlx` transaction
- [ ] Cache bust after `tx.commit()` in service layer, never before
- [ ] Redis invalidation in service orchestration layer (not repository)
- [ ] `X-User-Id` / `X-User-Roles` trusted from Traefik only (never from client)
- [ ] DB roles enforce schema-level access (auth_service_role, admin_service_role, driver_service_role)
- [ ] Analytics writes go to isolated `analytics_db` (never `platform_db`)
- [ ] All endpoints under `/api/v1/`
- [ ] POST endpoints require `Idempotency-Key` header
- [ ] JWT `aud` validated at Traefik against the calling client
- [ ] JWT never stored in `localStorage` (in-memory only)
- [ ] No `unwrap()` / `expect()` outside test code
- [ ] No raw SQL strings — `sqlx::query!` macros only

## Verification

- `cargo test` — all unit + integration tests pass
- `cargo clippy -- -D warnings` — zero warnings
- Dashboard login flow works end-to-end
- Partner CRUD creates DB rows + busts Redis + logs diff-based audit to analytics
- Traefik returns 401 on expired/malformed JWT
- Traefik returns 401 on mismatched `aud` claim
- Keycloak token endpoint unreachable from outside Auth Service network
- Idempotent POST with same key returns original response (not duplicate)
- DB role `driver_service_role` cannot write to `inventory` tables
- DB role `auth_service_role` cannot read `inventory` tables
