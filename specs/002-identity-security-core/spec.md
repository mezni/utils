# Feature Specification: Identity & Security Core

**Feature Branch**: `002-identity-security-core`

**Created**: 2026-06-21

**Status**: Draft

**Input**: Read from sprint 01 backlog — implement Keycloak identity, JWT validation, RBAC, and JIT provisioning across all services.

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Keycloak Identity Integration (Priority: P1)

Drivers, partners, and admins authenticate through Keycloak, which is the sole identity authority. Tokens are validated at the gateway and in each service.

**Why this priority**: Without identity, no user can access any feature. This is the foundational security layer.

**Independent Test**: Authenticate as each role (driver, partner, admin) and verify the JWT is accepted by all three services.

**Acceptance Scenarios**:

1. **Given** a user with valid credentials, **When** they authenticate via Keycloak, **Then** they receive a signed JWT containing their role and user ID
2. **Given** a user with an expired or invalid token, **When** they make a request to any service, **Then** the request is rejected with 401 Unauthorized
3. **Given** a valid JWT, **When** it passes through Traefik, **Then** the gateway validates the token before forwarding to the service

---

### User Story 2 — Role-Based Access Control (Priority: P1)

Each authenticated user has a role that determines what they can access. Drivers see driver features, admins see admin features, and partners see partner features.

**Why this priority**: Access control prevents unauthorized users from accessing restricted resources. Without RBAC, any authenticated user could access admin functionality.

**Independent Test**: Create three test accounts (driver, partner, admin) and verify each can only access their permitted endpoints.

**Acceptance Scenarios**:

1. **Given** an authenticated driver, **When** they call a driver endpoint, **Then** the request succeeds
2. **Given** an authenticated driver, **When** they call an admin-only endpoint, **Then** the request is rejected with 403 Forbidden
3. **Given** an authenticated admin, **When** they call any endpoint, **Then** the request succeeds (admin has broadest access)
4. **Given** an unauthenticated request, **When** it reaches any protected endpoint, **Then** the request is rejected with 401 Unauthorized

---

### User Story 3 — Just-In-Time User Provisioning (Priority: P1)

When a user authenticates for the first time, their profile is automatically created in platform_db. On subsequent logins, their profile is updated with the latest role and attributes from Keycloak.

**Why this priority**: Manual user provisioning would be error-prone and slow. JIT ensures the local database stays in sync with Keycloak without manual intervention.

**Independent Test**: Authenticate as a new user, then query the user_profiles table to verify the record was created with matching role and details.

**Acceptance Scenarios**:

1. **Given** a new user authenticates for the first time, **When** the auth-service processes the JWT, **Then** a new user_profiles record is created with their Keycloak UUID, role, and email
2. **Given** an existing user whose role changed in Keycloak, **When** they authenticate, **Then** their user_profiles record is updated with the new role
3. **Given** a user authenticates, **When** JIT provisioning completes, **Then** the user_profiles record matches the Keycloak identity exactly

---

### User Story 4 — Audit Logging for Security Events (Priority: P2)

All authentication events (login success, login failure, token rejection) are logged to analytics_db for security monitoring and compliance.

**Why this priority**: Audit trails are required for security incident investigation and compliance with data protection standards.

**Independent Test**: Trigger a login failure and a login success, then verify both events appear in the audit log.

**Acceptance Scenarios**:

1. **Given** a successful login, **When** the auth-service processes the authentication, **Then** a "login_success" audit event is written to analytics_db
2. **Given** a failed login attempt, **When** authentication fails, **Then** a "login_failure" audit event is written with the reason
3. **Given** a rejected JWT (expired/invalid), **When** the token is rejected, **Then** a "token_rejected" audit event is written

---

### User Story 5 — CI Security Gates (Priority: P2)

Four new CI gates enforce security policies: identity validation, Keycloak dependency, RBAC coverage, and session consistency.

**Why this priority**: Automated enforcement prevents security regressions from reaching production.

**Independent Test**: Introduce a deliberate policy violation and verify the CI gate catches it.

**Acceptance Scenarios**:

1. **Given** a service that imports Keycloak Admin API client, **When** it is not auth-service, **Then** the CI gate fails with a dependency violation
2. **Given** a new endpoint without a role guard, **When** the RBAC coverage check runs, **Then** it fails with an uncovered endpoint
3. **Given** a JWT role that does not match the platform_db role mapping, **When** the session consistency check runs, **Then** it fails with a mismatch
4. **Given** a CI run with all security gates passing, **When** the pipeline completes, **Then** all four gates report passed

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST integrate Keycloak as the sole identity provider for all services
- **FR-002**: System MUST create a Keycloak realm named `bornemap`
- **FR-003**: System MUST create Keycloak clients for each application: `mobile-driver`, `web-driver`, `admin-dashboard`
- **FR-004**: System MUST define exactly three roles: `driver`, `partner`, `admin`
- **FR-005**: System MUST validate all incoming JWTs against Keycloak JWKS endpoint before processing requests
- **FR-006**: System MUST implement gateway-level JWT validation (Traefik forward-auth)
- **FR-007**: System MUST implement service-level JWT validation middleware in all three services
- **FR-008**: System MUST extract role claims from JWTs and enforce RBAC on every protected endpoint
- **FR-009**: System MUST provision user profiles in platform_db on first valid JWT authentication (JIT provisioning)
- **FR-010**: System MUST upsert user profiles on every authentication to keep roles in sync with Keycloak
- **FR-011**: System MUST implement resource ownership checks for user-scoped resources
- **FR-012**: System MUST log all authentication events (success, failure, token rejection) to analytics_db
- **FR-013**: System MUST protect all API endpoints with role-based access guards
- **FR-014**: System MUST reject requests with missing, expired, or invalid JWTs with 401
- **FR-015**: System MUST reject requests with insufficient role permissions with 403
- **FR-016**: System MUST add CI gates: identity validation, Keycloak dependency check, RBAC coverage check, session consistency check
- **FR-017**: System MUST expose a Keycloak sync endpoint on auth-service for manual role/attribute synchronization

### Key Entities *(include if feature involves data)*

- **Keycloak Realm (`bornemap`)**: Central identity domain containing clients, roles, and user mappings
- **Keycloak Clients (`mobile-driver`, `web-driver`, `admin-dashboard`)**: OAuth2 clients representing each application
- **Roles (`driver`, `partner`, `admin`)**: Named permission sets that determine endpoint access
- **JWT Token**: Signed JSON Web Token containing user identity, role, and claims, validated via JWKS
- **User Profiles (platform_db.users.user_profiles)**: Local projection of Keycloak identity, created/updated via JIT provisioning
- **Audit Events (analytics_db.telemetry.raw_events)**: Immutable log of authentication and authorization events

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Users can authenticate via Keycloak and receive a valid JWT within 2 seconds
- **SC-002**: All three services validate JWTs and reject invalid tokens with 401 in under 100ms per request
- **SC-003**: RBAC is enforced on every API endpoint — no unprotected routes exist after deployment
- **SC-004**: New users are auto-provisioned in platform_db within 5 seconds of first successful authentication
- **SC-005**: All four CI security gates pass on every commit — zero regressions allowed
- **SC-006**: Audit log captures 100% of authentication events with no gaps
- **SC-007**: Users with expired roles are denied access within 10 minutes of role removal in Keycloak

## Assumptions

- Keycloak will be deployed as a Docker container alongside existing infrastructure (platform_db, analytics_db)
- Traefik will handle gateway-level JWT validation via forward-auth middleware
- Each service maintains its own JWT validation cache to avoid excessive JWKS fetches
- The three roles (driver, partner, admin) are mutually exclusive — a user has exactly one role
- JWT tokens have a maximum lifetime of 15 minutes (short-lived tokens)
- Keycloak Admin API is only called by auth-service, never by driver-service or admin-service
- Audit events use the existing telemetry.raw_events table in analytics_db
