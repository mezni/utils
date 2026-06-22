# Feature Specification: Identity & Security Core

**Feature Branch**: `002-identity-security-core`

**Created**: 2026-06-21

**Status**: Draft

**Input**: Read from sprint 01 backlog — implement Keycloak identity, JWT validation, RBAC, and JIT provisioning across all services.

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Keycloak Identity Integration (Priority: P1)

Drivers, partners, and admins authenticate through Keycloak, which is the sole identity and authorization authority. Tokens are validated at the gateway (Traefik) and in each service (defense-in-depth).

**Why this priority**: Without identity, no user can access any feature. This is the foundational security layer.

**Independent Test**: Authenticate as each role (driver, partner, admin) and verify the JWT is accepted by all three services.

**Acceptance Scenarios**:

1. **Given** a user with valid credentials, **When** they authenticate via Keycloak, **Then** they receive a signed JWT containing their role and user ID
2. **Given** a user with an expired or invalid token, **When** they make a request to any service, **Then** the request is rejected with 401 Unauthorized
3. **Given** a valid JWT, **When** it passes through Traefik, **Then** the gateway validates signature, issuer, audience, and expiration before forwarding
4. **Given** a valid JWT, **When** a service receives it, **Then** the service re-validates signature, issuer, audience, expiration, and not-before against Keycloak JWKS

---

### User Story 2 — Role-Based Access Control (Priority: P1)

Each authenticated user has a role that determines what they can access. Role decisions are based solely on JWT claims from Keycloak — platform_db role fields are projections only and never authoritative.

**Why this priority**: Access control prevents unauthorized users from accessing restricted resources. Without RBAC, any authenticated user could access admin functionality.

**Independent Test**: Create three test accounts (driver, partner, admin) and verify each can only access their permitted endpoints.

**Acceptance Scenarios**:

1. **Given** an authenticated driver, **When** they call a driver endpoint, **Then** the request succeeds
2. **Given** an authenticated driver, **When** they call an admin-only endpoint, **Then** the request is rejected with 403 Forbidden
3. **Given** an authenticated admin, **When** they call any endpoint, **Then** the request succeeds (admin inherits all lower-role permissions)
4. **Given** an unauthenticated request, **When** it reaches any protected endpoint, **Then** the request is rejected with 401 Unauthorized
5. **Given** a user whose Keycloak role changed but platform_db still has the old role, **When** they make a request, **Then** the JWT role is used for authorization (platform_db is never authoritative)

---

### User Story 3 — Just-In-Time User Provisioning (Priority: P1)

When any service encounters a JWT from an unknown user, it calls auth-service's identity sync endpoint to provision the user profile. On subsequent authentications, the profile is updated with the latest attributes from Keycloak.

**Why this priority**: Manual user provisioning would be error-prone and slow. JIT ensures the local database stays in sync with Keycloak without manual intervention.

**Independent Test**: Authenticate as a new user through any service, then query the user_profiles table to verify the record was created with matching role and details.

**Acceptance Scenarios**:

1. **Given** a new user authenticates for the first time, **When** any service detects a missing local profile, **Then** it calls auth-service sync endpoint which creates a new user_profiles record
2. **Given** an existing user whose role changed in Keycloak, **When** they authenticate, **Then** their user_profiles record is updated with the new role
3. **Given** a user authenticates, **When** JIT provisioning completes, **Then** the user_profiles record matches the Keycloak identity exactly
4. **Given** a request to driver-service from an unknown user, **When** the sync endpoint is unavailable, **Then** the request fails with 503 and the event is logged

---

### User Story 4 — Audit Logging for Security Events (Priority: P2)

All authentication and authorization events are published to the event bus (driver-service `POST /api/v1/telemetry/events`) and asynchronously persisted to analytics_db.

**Why this priority**: Audit trails are required for security incident investigation and compliance with data protection standards.

**Independent Test**: Trigger a login failure and a login success, then verify both events appear in the audit log.

**Acceptance Scenarios**:

1. **Given** a successful login, **When** auth-service processes the authentication, **Then** a "auth.login_success" event is published to the event bus
2. **Given** a failed login attempt, **When** authentication fails, **Then** an "auth.login_failure" event is published with the reason
3. **Given** a rejected JWT (expired/invalid), **When** the token is rejected, **Then** an "auth.token_rejected" event is published
4. **Given** an access denied response, **When** RBAC rejects a request, **Then** an "auth.access_denied" event is published
5. **Given** a role change is detected during JIT, **When** the profile is updated, **Then** an "auth.role_change_detected" event is published
6. **Given** any audit event, **When** it arrives at driver-service, **Then** it is deduplicated by idempotency_key and written to analytics_db.raw_events

---

### User Story 5 — CI Security Gates (Priority: P2)

Four new CI gates enforce security policies with machine-verifiable criteria: identity validation, Keycloak dependency, RBAC coverage, and session consistency.

**Why this priority**: Automated enforcement prevents security regressions from reaching production.

**Independent Test**: Introduce a deliberate policy violation and verify the CI gate catches it.

**Acceptance Scenarios**:

1. **Given** a controller endpoint that lacks a role guard decorator, **When** the RBAC coverage check runs, **Then** it fails with the specific endpoint listed
2. **Given** a Cargo.toml adding keycloak-client to a non-auth service, **When** the Keycloak dependency gate runs, **Then** it fails with the violating service name
3. **Given** a users.user_profiles row with a non-UUID primary key, **When** the identity validation gate runs, **Then** it fails with the violating table
4. **Given** a JWT role test vector that does not match the platform_db role, **When** the session consistency check runs, **Then** it fails with the mismatch details

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST integrate Keycloak as the sole identity and authorization authority for all services
- **FR-002**: System MUST create a Keycloak realm named `bornemap`
- **FR-003**: System MUST create Keycloak clients: `mobile-driver` (public, PKCE), `web-driver` (public, PKCE), `admin-dashboard` (confidential)
- **FR-004**: System MUST define exactly three roles with precedence: admin > partner > driver; admin inherits all, partner inherits driver only if explicitly granted
- **FR-005**: System MUST validate all incoming JWTs against Keycloak JWKS endpoint before processing requests
- **FR-006**: System MUST implement gateway-level JWT validation (Traefik forward-auth) covering signature, issuer, audience, and expiration
- **FR-007**: System MUST implement service-level JWT validation middleware in all three services covering signature, issuer, audience, expiration, and not-before
- **FR-008**: System MUST extract role claims from JWTs and enforce RBAC on every protected endpoint; authorization decisions MUST be based solely on JWT claims, not platform_db projections
- **FR-009**: System MUST provision user profiles in platform_db on first valid JWT authentication (JIT provisioning) via auth-service sync endpoint
- **FR-010**: System MUST upsert user profiles on every authentication to keep roles in sync with Keycloak
- **FR-011**: System MUST implement resource ownership checks — user-scoped resources MUST contain an owner_user_id; access is granted only when JWT.sub == owner_user_id or role == admin
- **FR-012**: System MUST publish all authentication and authorization events to the event bus (driver-service POST /api/v1/telemetry/events) for asynchronous persistence to analytics_db
- **FR-013**: System MUST protect all API endpoints with role-based access guards
- **FR-014**: System MUST reject requests with missing, expired, or invalid JWTs with 401
- **FR-015**: System MUST reject requests with insufficient role permissions with 403
- **FR-016**: System MUST add CI gates: identity validation, Keycloak dependency check, RBAC coverage check, session consistency check — each with machine-verifiable criteria
- **FR-017**: System MUST expose a Keycloak sync endpoint on auth-service for manual identity synchronization
- **FR-018**: Gateway and services MUST validate: signature, issuer (`iss`), audience (`aud`), expiration (`exp`), and not-before (`nbf`) against Keycloak realm metadata
- **FR-019**: Authorization decisions MUST be based solely on JWT role claims from Keycloak; platform_db role fields are projections only and MUST never be treated as the source of truth
- **FR-020**: Any service detecting a missing local user profile MUST invoke auth-service identity synchronization before processing the request
- **FR-021**: All service-to-service communication MUST use machine credentials (Keycloak service account client credentials or mTLS)
- **FR-022**: User-scoped resources MUST contain an `owner_user_id` mapped to the Keycloak subject; access is granted only when JWT.sub == owner_user_id or role == admin
- **FR-023**: Authentication and authorization events MUST be published to the event bus (driver-service POST /api/v1/telemetry/events) and persisted asynchronously into analytics_db; auth-service MUST NOT write directly to analytics_db
- **FR-024**: OIDC Authorization Code + PKCE MUST be used for user authentication; refresh tokens MUST be issued and managed by Keycloak (access tokens: 15 min, refresh tokens: 24 hours)
- **FR-025**: Client configuration MUST match application trust boundaries: `mobile-driver` and `web-driver` = public client with PKCE; `admin-dashboard` = confidential client
- **FR-026**: Role precedence: admin > partner > driver; admin inherits all permissions; partner inherits driver permissions only if explicitly granted
- **FR-027**: CI MUST fail if: (a) a controller endpoint lacks @Roles decorator, (b) an endpoint lacks auth middleware, (c) a route is absent from the RBAC matrix, (d) an identity violation is detected, (e) JWT role != platform_db role
- **FR-028**: JWKS keys MUST rotate automatically; Keycloak handles rotation, services MUST handle key changes by refreshing cache on unknown `kid`
- **FR-029**: JWT validation caches MUST refresh on unknown `kid` (fetch new JWKS from Keycloak when current key is not found)
- **FR-030**: Keycloak realm configuration MUST be exported to `infrastructure/keycloak/realm-export.json` and version-controlled
- **FR-031**: Service accounts (machine-to-machine) MUST use least-privilege roles
- **FR-032**: Security events MUST include: timestamp, subject UUID, role, event type, source IP, user agent, correlation ID, reason (for failures)
- **FR-033**: Every request MUST carry a correlation ID propagated across all services

### Key Entities *(include if feature involves data)*

- **Keycloak Realm (`bornemap`)**: Central identity domain containing clients, roles, and user mappings
- **Keycloak Clients (`mobile-driver` public PKCE, `web-driver` public PKCE, `admin-dashboard` confidential)**: OAuth2 clients representing each application
- **Roles (`driver`, `partner`, `admin`)**: Hierarchical permission sets (admin > partner > driver) that determine endpoint access; defined and managed in Keycloak
- **JWT Token**: Signed JSON Web Token containing user identity, role, and claims, validated via JWKS; sole determinant of authorization decisions
- **User Profiles (platform_db.users.user_profiles)**: Local projection of Keycloak identity, created/updated via JIT provisioning; NEVER authoritative for authorization
- **Audit Events (event bus → analytics_db)**: Immutable log of authentication and authorization events published via the event bus

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Users can authenticate via Keycloak using OIDC PKCE and receive a valid JWT within 2 seconds
- **SC-002**: All three services + gateway validate JWTs and reject invalid tokens with 401 in under 100ms per request
- **SC-003**: RBAC is enforced on every API endpoint — no unprotected routes exist after deployment; verified by CI gate
- **SC-004**: New users are auto-provisioned in platform_db within 5 seconds of first authentication through any service
- **SC-005**: All four CI security gates pass on every commit — zero regressions allowed
- **SC-006**: Audit log captures 100% of authentication and authorization events with no gaps
- **SC-007**: Role changes in Keycloak become effective no later than the user's current JWT expiration, or within 5 minutes if short-lived tokens are used

## Assumptions

- Keycloak will be deployed as a Docker container alongside existing infrastructure (platform_db, analytics_db)
- Traefik will handle gateway-level JWT validation via forward-auth middleware
- Each service maintains its own JWT validation cache to avoid excessive JWKS fetches
- The three roles (driver, partner, admin) follow the precedence: admin > partner > driver
- Access tokens: 15 minutes; Refresh tokens: 24 hours
- Keycloak Admin API is only called by auth-service, never by driver-service or admin-service
- Audit events use the event bus pattern: auth-service → driver-service POST /api/v1/telemetry/events → analytics_db
- Every request carries a correlation ID for tracing and deduplication
