# BorneMap — Sprint Backlog

> Granular tasks assigned to builder sessions. One ticket = one LLM session.
> For high-level tracking, see `roadmap_status.md`.

---

## MVP-1: Admin Flow System

**Scope:** Admin → Traefik → Auth Service → Keycloak → Admin Service → PostGIS + Redis + analytics_db

---

### 🚀 Sprint 0 — Platform Bootstrap

**Goal:** System runnable locally with all dependencies wired, no business logic.

| ID | Ticket | Est. effort | Dependencies |
|----|--------|-------------|-------------|
| INF-1 | Docker Compose base stack (Postgres 16+PostGIS, Redis, Keycloak, Traefik) | 🟢 Done | MVP-0 |
| INF-2 | Keycloak bootstrap (realm: bornemap, clients, roles) | 🟢 Done | INF-1 |
| INF-3 | DB schema bootstrap (schemas, partners, stations, chargers, lookup tables) | 🟢 Done | INF-1 |
| INF-4 | Traefik routing (no auth yet — /api/v1/auth → auth, /admin → admin, /driver → driver) | 🟢 Done | INF-1 |

**Exit:** All services start via docker-compose, DB reachable, Keycloak admin UI accessible, Traefik routes correctly.

---

### 🔐 Sprint 1 — Auth Service + Keycloak Integration

**Goal:** Fully working authentication pipeline through Auth Service only.

**Status:** 🟢 Complete — All 44 tasks finished including Dockerfile, integration tests, load testing, and security verification. Code compiles successfully.

| ID | Ticket | Priority | Dependencies | Status |
|----|--------|----------|-------------|--------|
| AUTH-1 | Login endpoint (`POST /api/v1/auth/login`, Keycloak token exchange) | P1 | INF-2 | 🟢 Done |
| AUTH-2 | Refresh endpoint (`refresh_token` rotation) | P1 | AUTH-1 | 🟢 Done |
| AUTH-3 | User sync layer (upsert into `users.USR_`) | P2 | INF-3 | 🟢 Done |
| AUTH-4 | JWT validation utilities (shared parsing) | P1 | AUTH-1 | 🟢 Done |
| AUTH-5 | DB role setup (auth_service_role, users schema-only) | P1 | INF-3 | 🟢 Done |
| AUTH-6 | Logout endpoint (revoke Keycloak session, idempotent) | P1 | AUTH-1 | 🟢 Done |
| AUTH-7 | Audience claim extraction and propagation | P1 | AUTH-4 | 🟢 Done |
| AUTH-8 | Log redaction middleware (FR-001) | P1 | - | 🟢 Done |
| AUTH-9 | Rate limiting middleware (10/min on /login) | P1 | - | 🟢 Done |
| AUTH-10 | Token validation before Keycloak calls | P1 | - | 🟢 Done |
| AUTH-11 | Production Dockerfile (multi-stage, distroless) | P1 | - | 🟢 Done |
| AUTH-12 | Request body size limits (4MB) and timeouts | P1 | - | 🟢 Done |
| AUTH-13 | Error response hardening (no Keycloak URLs) | P1 | - | 🟢 Done |
| AUTH-14 | Integration tests (T026a-f) | P1 | - | 🟢 Done |
| AUTH-15 | Load test script (100 concurrent requests) | P1 | - | 🟢 Done |
| AUTH-16 | SC-004 verification procedure | P1 | - | 🟢 Done |
| AUTH-17 | User Profile DDL migration | P1 | - | 🟢 Done |
| AUTH-18 | GET /api/v1/auth/me endpoint | P2 | - | 🟢 Done |

**Exit:** Login creates USR- row, refresh works end-to-end, DB role restricts to `users` schema.

---

### 🧠 Sprint 2 — Admin Service Core (CRUD + Transactions)

**Goal:** Admin can create partners, stations, chargers with safe transactions.

| ID | Ticket | Est. effort | Dependencies |
|----|--------|-------------|-------------|
| ADM-1 | Partner CRUD (create/update endpoints) | 3h | INF-3 |
| ADM-2 | Station CRUD (create/update endpoints) | 3h | INF-3 |
| ADM-3 | Charger CRUD (create/update endpoints) | 3h | INF-3 |
| ADM-4 | Transaction orchestrator (BEGIN → WRITE → COMMIT, rollback safety) | 2h | ADM-1/2/3 |
| ADM-5 | DB role enforcement (admin_service_role, inventory-only access) | 0.5h | INF-3 |

**Exit:** Partner/station/charger CRUD via API, transactional writes, unauthorized schema access blocked.

---

### ⚡ Sprint 3 — Security Layer (Gateway + Identity Enforcement)

**Goal:** Secure and production-safe at ingress level.

| ID | Ticket | Est. effort | Dependencies |
|----|--------|-------------|-------------|
| SEC-1 | JWKS validation middleware (signature, caching 10-30min TTL) | 2h | INF-4 |
| SEC-2 | JWT claims validation (exp, iss, aud checks) | 1h | SEC-1 |
| SEC-3 | Audience enforcement (admin-dashboard for admin routes) | 1h | SEC-2 |
| SEC-4 | Header injection (`X-User-Id`, `X-User-Roles`) | 0.5h | SEC-1 |
| SEC-5 | Keycloak network isolation (block external `/realms/*` access) | 0.5h | INF-1 |

**Exit:** Invalid JWT → 401, wrong audience → blocked, Keycloak unreachable except from Auth Service.

---

### 🧱 Sprint 4 — Redis + Cache Invalidation System

**Goal:** Spatial read acceleration + strict invalidation model.

| ID | Ticket | Est. effort | Dependencies |
|----|--------|-------------|-------------|
| REDIS-1 | Redis client integration in Admin Service (connection pool, key strategy) | 1h | INF-1 |
| REDIS-2 | Cache key design (`stations:tile:{z}:{x}:{y}`, `stations:near:{lat}:{lng}:{radius}`) | 0.5h | REDIS-1 |
| REDIS-3 | Invalidation service layer (post-commit bust logic, failure policy: log + header, no rollback) | 1.5h | REDIS-2, ADM-4 |
| REDIS-4 | MV refresh integration (`REFRESH MATERIALIZED VIEW CONCURRENTLY` in same orchestration step) | 1h | REDIS-3, INF-3 |
| REDIS-5 | Driver read integration (basic — verify cache hit/miss, placeholder handler) | 1h | REDIS-2 |

**Exit:** Admin update triggers cache invalidation + MV refresh after commit, Redis failure does not roll back DB.

---

### 📊 Sprint 5 — Analytics + Audit System

**Goal:** Track all mutations with BEFORE/AFTER diff logging.

| ID | Ticket | Est. effort | Dependencies |
|----|--------|-------------|-------------|
| AUD-1 | analytics_db setup + audit_log schema | 1h | INF-1 |
| AUD-2 | BEFORE/AFTER diff generator in service layer | 1.5h | ADM-4 |
| AUD-3 | Admin mutation logging hook (partner/station/charger, computed in service layer) | 1.5h | AUD-2, ADM-1/2/3 |
| AUD-4 | Index strategy on audit_log (actor_id, target_type+target_id, created_at DESC) | 0.5h | AUD-1 |

**Exit:** Every mutation logged with before/after diff, queryable audit trail exists.

---

### 🔁 Sprint 6 — Idempotency + Hardening

**Goal:** Admin APIs safe for retries and production traffic.

| ID | Ticket | Est. effort | Dependencies |
|----|--------|-------------|-------------|
| SAFE-1 | Idempotency middleware (UUID v4 keys, 24h cache store, deterministic replay) | 2h | ADM-4 |
| SAFE-2 | Duplicate detection logic (replayed key → return original response + `Idempotency-Replayed: true`) | 1h | SAFE-1 |
| SAFE-3 | Validation layer (reject invalid payloads, enforce constraint rules) | 1.5h | ADM-1/2/3 |
| SAFE-4 | Error contract standardization (401/403/404/409/410/500 per spec table) | 1h | AUTH-1, ADM-1/2/3 |

**Exit:** Duplicate POST does not create duplicates, same key returns same response, all error codes match spec.

---

### 🧪 Sprint 7 — End-to-End System Integration

**Goal:** Full Admin flow working end-to-end.

| ID | Ticket | Est. effort | Dependencies |
|----|--------|-------------|-------------|
| E2E-1 | Full login flow test (Dashboard → Auth → Keycloak → USR- upsert) | 1h | All above |
| E2E-2 | Partner creation flow test (CRUD + audit + Redis bust + MV refresh) | 1h | All above |
| E2E-3 | Station update flow test (CRUD + audit + Redis bust + MV refresh) | 1h | All above |
| E2E-4 | Redis invalidation verification (after commit, not before; Redis failure path) | 1h | All above |
| E2E-5 | Audit logging verification (before/after diff correctness) | 0.5h | All above |
| E2E-6 | Gateway security tests (401 expired, 401 wrong aud, 403 wrong role, 503 Keycloak down) | 1.5h | All above |

**Exit:** Full system works without bypass, all constraints enforced, no direct DB/service violations.

---

## Legend

| Icon | Meaning |
|------|---------|
| 🟢 Done | Built, tested, verified |
| 🟡 In progress | Being worked on this session |
| ⬜ Not started | Not yet begun |
| 🔴 Blocked | Waiting on dependency or decision |
