# Research: Identity Core (MVP-2)

## Rust Keycloak Integration

**Decision**: Direct HTTP calls to Keycloak REST API via `reqwest` + local JWT validation via `jsonwebtoken`

**Rationale**:
- Rust has no official Keycloak adapter. The `keycloak` crate exists but is immature and adds unnecessary abstraction
- Keycloak's REST API (Admin API + OpenID Connect endpoints) is well-documented and stable
- JWT validation with JWKS caching is straightforward with the `jsonwebtoken` crate
- Auth-service needs only 3 Keycloak interactions: user creation (Admin API), token exchange (OIDC), and JWKS fetching (OIDC)

**Alternatives considered**:
- `keycloak` crate: Adds dependency risk, limited community adoption, hides HTTP details that we need for error handling
- `openidconnect` crate: More OIDC than we need; auth-service delegates the auth flow to Keycloak, not implement it

## Shared Identity Library

**Decision**: Create `libs/identity-core` in the services workspace

**Rationale**:
- Every service needs to validate JWTs and extract identity claims (FR-012, FR-013)
- JWKS caching, token parsing, and claim extraction are cross-cutting concerns
- Following the established pattern: `libs/geo-core`, `libs/db-core` already exist

**Crate responsibilities**:
- `JwtValidator`: Fetches and caches JWKS, validates tokens, extracts claims
- `IdentityClaims`: Typed struct for decoded token claims (sub, email, realm, roles, usr_id)
- `KeycloakAdminClient`: Thin wrapper for Keycloak Admin API calls (create user, assign roles)

## JWT Validation with JWKS

**Decision**: Pull-based JWKS caching with 5-minute TTL

**Rationale**:
- Keycloak exposes a standard JWKS endpoint at `/realms/{realm}/protocol/openid-connect/certs`
- `jsonwebtoken` crate supports RS256/RS384/RS512 and JWKS-based validation natively
- 5-minute TTL balances freshness with minimizing Keycloak requests
- On validation failure, re-fetch JWKS immediately (handles key rotation gracefully)

**Validation flow**:
1. Extract token from `Authorization: Bearer <token>` header
2. Parse token header to get `kid`
3. Look up `kid` in cached JWKS
4. If missing, refresh JWKS from Keycloak
5. Validate signature, expiry, issuer, audience
6. Extract custom claims (realm, roles, usr_id)
7. Return `IdentityClaims` or error

## Rate Limiting

**Decision**: In-memory rate limiting via `tokio::sync::RwLock<HashMap>`

**Rationale**:
- Article VII bans Redis/kafka/queues — no external cache available
- MVP-2 scale (single instance) makes in-memory sufficient
- `tokio::sync` primitives are zero-dependency and well-tested

**Implementation**:
- Per-IP counter: key = `rate_limit:{ip}:login`, TTL = 60s sliding window
- Per-account counter: key = `rate_limit:{email}:login`, TTL = 15min sliding window
- Instead of TTL-based expiry (needs background task), use: track tuples of `(Instant, count)` and evict entries older than the window on each request
- Cleanup: scan and evict stale entries every 60s via a tokio spawn task

**Alternatives considered**:
- `governor` crate: Feature-rich but heavyweight for our simple two-tier approach
- `arc-switch` + `dashmap`: Overkill for single-instance MVP-2

## User Schema

**Decision**: `users` schema with 5 tables as specified

**Rationale**:
- Isolates identity from `inventory` per MVP-2 architecture
- `accounts` as master identity ledger with USR-nanoid identifiers
- `roles` and `account_roles` for RBAC (many-to-many)
- `identity_providers` for federated identity (LOCAL/GOOGLE/APPLE)
- `audit_log` for compliance and debugging

**Unaddressed edge cases** (resolved):
- Registration with previously-used email (DISABLED account): If the email exists on a DISABLED account, registration re-activates the account. If it exists on an ACTIVE account, return "email already in use". This is handled at the auth-service layer checking against `users.accounts`.
- Realm changes: Out of scope for MVP-2. The spec has no user story requiring realm migration. When it becomes needed, it will be an UPDATE to `accounts.realm` + Keycloak user group membership change.
- Concurrent login attempts: Handled by rate limiting at both IP and account level. The 20-failure/15min cooldown prevents brute force regardless of concurrency.

## Keycloak Realm/Client/Role Configuration

**Decision**: Shell scripts using `kcadm.sh` in `source/infra/keycloak/`

**Rationale**:
- kcadm.sh is Keycloak's official admin CLI, included in the Keycloak Docker image
- Scripts can be run during deployment and are idempotent (check existence before create)
- Configuration as code — auditable and reproducible

**Scripts needed**:
- `init-keycloak.sh`: Authenticates kcadm, creates realms, clients, roles
- `create-realm.sh <realm>`: Creates a realm (bm-drivers, bm-control)
- `create-client.sh <realm> <client-id>`: Creates a confidential client with service account
- `create-role.sh <realm> <role-name>`: Creates a realm role (registered_driver, partner, admin)

## Authorization Code + PKCE via BFF

**Decision**: Auth-service acts as BFF (Backend for Frontend)

**Rationale**:
- FR-011 mandates browser redirect flow, not direct credential submission
- BFF pattern keeps client secrets server-side
- Auth-service handles the OIDC token exchange, returning a session cookie to the client
- Simplifies mobile/web clients — they just redirect to auth-service endpoints

**Flow**:
1. Client redirects to `/auth/login` on auth-service
2. Auth-service redirects to Keycloak with PKCE challenge
3. Keycloak authenticates user, redirects back to auth-service with auth code
4. Auth-service exchanges auth code + PKCE verifier for tokens
5. Auth-service sets a session cookie (HttpOnly, Secure, SameSite=Lax)
6. Client uses session cookie for subsequent API requests
7. Auth-service validates the session cookie, returns the JWT for downstream services

## Unresolved Edge Cases for MVP-2

- **Email deactivation re-registration**: If a DISABLED account's email is used for registration, re-activate the account. Documented as EDGE-1.
- **Realm change**: Not in scope. Documented as FUTURE-1.
- **Concurrent logins**: No explicit restriction in MVP-2. Same session can be used from multiple devices. Documented as EDGE-2.
