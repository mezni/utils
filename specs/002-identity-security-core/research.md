# Research Report: Identity & Security Core

**Date**: 2026-06-21
**Branch**: `002-identity-security-core`
**Spec**: [spec.md](./spec.md)

## 1. Analytics Write Gate: Auth Audit Events

**Question**: How should auth-service write audit events to analytics_db without violating the single-writer rule?

### Options Evaluated

| Approach | Constitutional Integrity | Complexity | Latency | Reliability |
|---|---|---|---|---|
| 1. Narrow Exception (direct write) | ❌ Violates §4.3 + §5 | Low | ~1ms | High |
| 2. Event Bus Routing (BUS) | ✅ Follows existing pattern | Medium | ~5-15ms | Medium-High |
| 3. Batch Sync (staging table) | ⚠️ Violates spirit | High | Minutes | Medium |

### Decision: Approach 2 — Event Bus Routing (BUS)

**Rationale**: The constitution already defines the BUS pattern (`ADMIN → BUS → ADB`) in §11.4. Auth-service sends audit events to driver-service's `POST /api/v1/telemetry/events` endpoint. Driver-service handles deduplication and writing to analytics_db. This preserves single-writer integrity without constitutional amendment.

**Implementation**: Auth-service emits events as async HTTP POST with retry (3 attempts, exponential backoff) and in-memory ring buffer fallback.

## 2. JWT Validation Strategy

**Question**: Should JWT validation be in a shared crate or per-service?

**Options Evaluated**:
- **Shared crate (shared-infra)**: Clean DRY but risks forbidden edge violations (service depends on shared-infra, shared-infra depends on nothing → safe)
- **Per-service copy**: Duplicated code, but zero dependency risk
- **domain-types + standalone validator**: JWT types in domain-types, validation logic in a separate crate under `apps/packages/`

**Decision**: JWT claim structs and Role enum in `domain-types` (contracts-first). JWT validation middleware implemented in each service independently to avoid introducing a runtime dependency that could violate forbidden edges.

## 3. Keycloak Deployment Model

**Decision**: Keycloak runs as a Docker container in `infrastructure/docker-compose/local.yml` with its own PostgreSQL database (keycloak_db). Realm configuration is exported to `infrastructure/keycloak/realm-export.json` for version-controlled setup.

## 4. CI Security Gate Implementation

**Decision**: Four new CI validation scripts under `tools/`:
- `ci_gate_identity.sh` — Identity validation gate (CI-1.1)
- `ci_gate_keycloak.sh` — Keycloak dependency gate (CI-1.2)
- `ci_gate_rbac.sh` — RBAC coverage check (CI-1.3)
- `ci_gate_session.sh` — Session consistency check (CI-1.4)

Each script follows the same JSON artifact pattern as existing CI scripts.

## 5. Role Model

**Decision**: Three immutable roles — `driver`, `partner`, `admin`. Mutually exclusive (one user, one role). Roles are stored as PostgreSQL ENUM or VARCHAR with CHECK constraint in `users.user_profiles.role`.

## 6. JWT Token Lifetime

**Decision**: Access tokens: 15 minutes. Refresh tokens: 24 hours. Short-lived access tokens minimize the window for token misuse while keeping UX acceptable.
