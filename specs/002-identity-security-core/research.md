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

**Implementation**: Auth-service emits events as async HTTP POST with retry (3 attempts, exponential backoff) and in-memory ring buffer fallback. All calls to driver-service are authenticated via auth-service-sa Keycloak service account credentials (client_credentials grant). Driver-service validates the machine JWT before accepting events.

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

**Decision**: Three roles with explicit precedence — `admin > partner > driver`. Admin inherits all permissions. Partner inherits driver permissions only if explicitly granted. Roles are stored as VARCHAR with CHECK constraint in `users.user_profiles.role`. **Keycloak JWT role is authoritative for authorization decisions; platform_db role is a projection only and MUST NOT be used for authorization.** This prevents stale DB roles from granting inappropriate access.

## 6. JWT Token Lifetime

**Decision**: Access tokens: 15 minutes. Refresh tokens: 24 hours (OIDC PKCE flow). Short-lived access tokens minimize the window for token misuse. Role revocation takes effect at JWT expiration (max 15 min), or sooner with token introspection.

## 7. JIT Provisioning Flow

**Decision**: JIT provisioning is triggered by ANY service detecting a missing user profile, not just auth-service. Services call auth-service `GET /api/v1/auth/sync?user_uuid={uuid}` (authenticated via machine credentials) which performs the upsert. This decouples JIT from the authentication path and ensures user profiles exist regardless of which service receives the request.

## 8. Client Type Definitions

**Decision**: 
- `mobile-driver`: Public client, PKCE enabled
- `web-driver`: Public client, PKCE enabled
- `admin-dashboard`: Confidential client (client secret)
- `auth-service-sa`: Confidential service account (client_credentials) for machine-to-machine auth
- `driver-service-sa`: Confidential service account
- `admin-service-sa`: Confidential service account

## 9. Service-to-Service Authentication

**Decision**: All internal API calls use Keycloak service account credentials (client_credentials grant). Each service has its own service account with least-privilege roles. This prevents unauthenticated internal access and provides audit trail for service interactions.

## 10. Role Change Propagation

**Decision**: SC-007 corrected to: role changes become effective no later than the user's current JWT expiration (max 15 minutes with 15 min access tokens). No token introspection is implemented in MVP — relying on short-lived tokens is sufficient.
