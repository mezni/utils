# Data Model: Identity & Security Core

**Date**: 2026-06-21
**Branch**: `002-identity-security-core`
**Spec**: [spec.md](./spec.md)

---

## Entities

### UserProfile

**Table**: `users.user_profiles` (platform_db)

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| user_id | UUID | PRIMARY KEY | Keycloak UUID (identity dual system) |
| email | VARCHAR(255) | UNIQUE, NOT NULL | User email from Keycloak |
| role | VARCHAR(20) | NOT NULL, CHECK (role IN ('driver','partner','admin')) | Assigned role |
| display_name | VARCHAR(255) | NULL | User display name |
| phone | VARCHAR(50) | NULL | Contact phone |
| locale | VARCHAR(10) | DEFAULT 'en' | User locale preference |
| is_active | BOOLEAN | DEFAULT true | Soft delete flag |
| last_login_at | TIMESTAMPTZ | NULL | Last successful authentication timestamp |
| created_at | TIMESTAMPTZ | DEFAULT NOW() | Record creation timestamp |
| updated_at | TIMESTAMPTZ | DEFAULT NOW() | Last update timestamp |

**Indexes**:
- `idx_user_profiles_email` on (email)
- `idx_user_profiles_role` on (role)

**Identity**: UUID (Keycloak-assigned, per identity dual system rule)

**Notes**:
- Records are created by JIT provisioning on first authentication
- Updated on every authentication to sync role/attributes from Keycloak
- NOT a source of truth — Keycloak is authoritative

---

### AuditEvent

**Table**: `telemetry.raw_events` (analytics_db)

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| event_id | VARCHAR(15) | PRIMARY KEY | Event identifier (EVT-nanoid12) |
| event_type | VARCHAR(100) | NOT NULL | Event type (auth.login_success, auth.login_failure, auth.token_rejected, auth.access_denied, auth.role_change_detected, auth.jit_user_created, auth.jit_user_updated, auth.logout, auth.refresh_token_rejected) |
| source_service | VARCHAR(100) | NOT NULL | Originating service (auth-service) |
| event_data | JSONB | NOT NULL | Event payload (see contracts below) |
| idempotency_key | VARCHAR(255) | UNIQUE, NOT NULL | Deduplication key |
| created_at | TIMESTAMPTZ | DEFAULT NOW() | Event timestamp |

**Indexes**:
- `idx_audit_events_type` on (event_type)
- `idx_audit_events_created_at` on (created_at DESC)
- `idx_audit_events_idempotency` on (idempotency_key)

**Identity**: EVT-nanoid(12) per entity identity convention

---

### KeycloakRealm

**Entity**: `bornemap` realm (Keycloak-managed)

| Component | Name | Type | Purpose |
|-----------|------|------|---------|
| Realm | bornemap | — | Central identity domain |
| Client | mobile-driver | Public, PKCE | Driver mobile app OAuth2 client |
| Client | web-driver | Public, PKCE | Driver web app OAuth2 client |
| Client | admin-dashboard | Confidential | Admin dashboard OAuth2 client |
| Client | auth-service-sa | Confidential (service account) | auth-service machine credentials |
| Client | driver-service-sa | Confidential (service account) | driver-service machine credentials |
| Client | admin-service-sa | Confidential (service account) | admin-service machine credentials |
| Role | driver | — | Standard driver access (lowest precedence) |
| Role | partner | — | Partner management access (middle precedence) |
| Role | admin | — | Full administrative access (highest precedence, inherits all) |

**Identity**: Managed by Keycloak (not platform_db)

---

## Relationships

```
Keycloak (bornemap realm) — sole identity & authorization authority
    │
    ├── Issues user JWTs (OIDC PKCE, 15 min access, 24h refresh)
    ├── Issues machine credentials (service account client credentials)
    │
    ▼
Clients → Traefik (gateway JWT validation: sig, iss, aud, exp)
    │
    ▼
┌─────────────────────────────────────────────────────┐
│ Any service detects JWT from unknown user           │
│   ↓                                                 │
│ Calls auth-service /api/v1/auth/sync (machine auth) │
│   ↓                                                 │
│ auth-service JIT upserts platform_db.user_profiles  │
│   ↓                                                 │
│ Returns profile to calling service                  │
└─────────────────────────────────────────────────────┘

auth-service ──→ Event Bus (POST /api/v1/telemetry/events)
    │                  │
    │                  ▼
    │           driver-service
    │                  │
    │                  ▼
    │          analytics_db.raw_events
    │
    ├── Reads/Writes platform_db.users (JIT projection)
    └── Calls Keycloak Admin API (sync, lookup)

driver-service ──→ Calls auth-service sync endpoint (machine auth)
    │                  │
    │                  ▼
    │          platform_db.user_profiles (read)
    │
    └── Reads gis schema, writes analytics_db

admin-service ──→ Calls auth-service sync endpoint (machine auth)
    │                  │
    │                  ▼
    │          platform_db.user_profiles (read)
    │
    └── Reads/writes inventory, reads analytics_db (via BUS)
```

## State Transitions

### Authentication Flow (with JIT)

```
End User → mobile-driver (PKCE auth)
    ↓
Keycloak verifies credentials → issues JWT (15 min access, 24h refresh)
    ↓
Traefik validates JWT: signature, iss, aud, exp
    ↓
driver-service validates JWT: signature, iss, aud, exp, nbf
    ↓
driver-service checks local user_profiles cache — MISS
    ↓
driver-service calls auth-service /api/v1/auth/sync?user_uuid={sub}
    (authenticated via auth-service-sa machine credentials)
    ↓
auth-service JIT: SELECT or INSERT/UPDATE user_profiles
    ↓
auth-service publishes auth.jit_user_created to event bus
    ↓
auth-service returns user profile to driver-service
    ↓
driver-service processes request with RBAC
    ↓
auth-service publishes auth.login_success to event bus
    ↓
driver-service deduplicates by idempotency_key
    ↓
driver-service writes: INSERT telemetry.raw_events
```

### Role Changes

```
Keycloak admin changes user role
    ↓
User next authenticates → JWT contains new role
    ↓
JIT provisioning UPSERTs user_profiles.role
    ↓
RBAC middleware now enforces new role on subsequent requests
```

## Validation Rules

- **User role**: MUST be one of 'driver', 'partner', 'admin'; precedence: admin > partner > driver
- **Authorization authority**: Keycloak JWT role is authoritative; platform_db role is a projection and MUST NOT be used for authorization decisions
- **JWT sub**: MUST be a valid UUID v4
- **JWT validation**: All services MUST verify: signature (JWKS), issuer (`iss`), audience (`aud`), expiration (`exp`), not-before (`nbf`)
- **Event idempotency_key**: MUST be unique per event to prevent duplicate audit entries
- **Audit event_type**: MUST follow `{service}.{action}` convention (e.g., `auth.login_success`)
- **Token lifetime**: Access tokens ≤ 15 minutes, Refresh tokens ≤ 24 hours
- **Service-to-service auth**: All internal API calls MUST use Keycloak service account credentials (client_credentials grant)
- **Client types**: `mobile-driver` and `web-driver` = public client with PKCE; `admin-dashboard` = confidential client
- **JWKS rotation**: Services MUST refresh JWKS cache on unknown `kid`
- **Correlation ID**: Every request MUST carry a unique correlation ID propagated across all services
- **Security event fields**: Every audit event MUST include: timestamp, subject UUID, role, event type, source IP, user agent, correlation ID, reason (for failures)
