# Feature Specification: Auth Service — Login, Refresh, Logout & Profile

**Feature Branch**: `002-auth-service`

**Created**: 2026-06-19

**Status**: Draft

**Input**: User description: "docs/specs/mvp-1-admin-flow.md Sprint 1"

## Clarifications

### Session 2026-06-19

- Q: Should a logout/token-revocation endpoint be included in Sprint 1? → A: Yes, include logout in Sprint 1.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Admin/partner logs in via dashboard (Priority: P1)

An admin or partner operator opens the dashboard, enters their credentials (email and password), and clicks "Sign In". The system authenticates them against the identity provider and returns a session token. The user is then redirected to the dashboard home page.

**Why this priority**: Without login, no user can access any protected feature. This is the gate for the entire platform.

**Independent Test**: Can be fully tested by navigating to the login page, submitting valid credentials, and receiving a signed token that grants access to protected resources.

**Acceptance Scenarios**:

1. **Given** a registered user with valid credentials, **When** they submit their email and password, **Then** they receive an access token and a refresh token, and their user profile is synced to the platform database.
2. **Given** a registered user, **When** they submit an incorrect password, **Then** they receive a clear error message and no token.
3. **Given** an unregistered user, **When** they submit any credentials, **Then** they receive an authentication error.

---

### User Story 2 - Client refreshes an expired session (Priority: P1)

A user's access token has expired while they are actively using the dashboard. The application automatically sends the stored refresh token to obtain a new access token without requiring the user to re-enter their credentials.

**Why this priority**: Users expect to remain logged in during a session without interruption. Manual re-authentication after every token expiry creates a poor experience.

**Independent Test**: Can be fully tested by obtaining a valid token pair, waiting for the access token to expire, submitting the refresh token, and verifying a new valid access token is returned.

**Acceptance Scenarios**:

1. **Given** a valid refresh token, **When** it is submitted to the refresh endpoint, **Then** a new access token and refresh token are returned.
2. **Given** an expired or revoked refresh token, **When** it is submitted, **Then** an error is returned and the user must re-authenticate.

---

### User Story 3 - User profile is synchronised on authentication (Priority: P2)

When a user logs in or refreshes their token, the system automatically creates or updates their user profile record in the platform database, ensuring profile data stays current across sessions.

**Why this priority**: Profile sync ensures downstream services have accurate user information without requiring a separate profile management flow.

**Independent Test**: Can be fully tested by logging in as a new user, verifying a profile record was created, then updating the user's attributes in the identity provider, logging in again, and confirming the profile record reflects the changes.

**Acceptance Scenarios**:

1. **Given** a user logging in for the first time, **When** authentication succeeds, **Then** a new user profile is created in the platform database.
2. **Given** an existing user logging in again, **When** authentication succeeds, **Then** their user profile is updated to reflect any changes.

---

### User Story 4 - User logs out and revokes their session (Priority: P1)

A user clicks "Sign Out" in the dashboard. The system revokes their active session tokens with the identity provider, making them immediately unusable. The user is redirected to the login page and must re-authenticate to access protected resources.

**Why this priority**: Without server-side logout, tokens remain valid until expiry even after a user signs out, creating a security gap. The constitution mandates that all auth flows (login, refresh, logout) route through the Auth Service.

**Independent Test**: Can be fully tested by logging in, obtaining a valid token pair, calling the logout endpoint with the refresh token, then attempting to use the same refresh token to obtain a new access token — must be rejected.

**Acceptance Scenarios**:

1. **Given** an authenticated user with valid tokens, **When** they call the logout endpoint with their refresh token, **Then** the refresh token is revoked and no new access token can be obtained using it.
2. **Given** an already-expired refresh token, **When** it is submitted to the logout endpoint, **Then** the server still acknowledges the logout (no error) since the session is effectively already terminated.

---

### User Story 5 - Client retrieves authenticated profile (Priority: P2)

A user opens the dashboard or mobile app and the application needs to verify the user's identity, roles, and profile data without decoding JWTs on the client. The app sends the access token to the Auth Service, which validates it and returns the synchronized user profile from the platform database.

**Why this priority**: Dashboard bootstrapping, role-gating for UI elements, and mobile auth flows all benefit from a dedicated profile endpoint. Decoupling profile resolution from JWT decoding simplifies frontend code and centralizes profile access.

**Independent Test**: Login to obtain an access token, call `GET /api/v1/auth/me` with the token, verify the response contains the correct user profile (id, email, roles). Repeat with an expired token and verify 401.

**Acceptance Scenarios**:

1. **Given** an authenticated user, **When** they call `/me` with a valid access token, **Then** they receive their user profile (id, email, roles) from the platform database.
2. **Given** an expired or invalid access token, **When** they call `/me`, **Then** they receive a 401 error.

---

### Edge Cases

- What happens when the identity provider is unreachable? The system must return a clear service-unavailable error rather than timing out indefinitely or exposing internal errors.
- How does the system handle malformed or incomplete requests? It must return a structured validation error identifying the specific issues.
- What happens if a client submits an invalid token format? The system must reject it with an appropriate error before any identity provider call.
- What happens if a logout is requested with an already-expired or revoked token? The logout endpoint must still return success — the session is already effectively terminated.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The system MUST provide a login endpoint that accepts user credentials and authenticates them against the identity provider before returning tokens. Credentials, `access_token`, and `refresh_token` must never be logged or stored. Only trace IDs and request metadata may appear in logs.
- **FR-002**: The system MUST provide a token refresh endpoint that accepts a refresh token and returns a new access token and refresh token.
- **FR-003**: The system MUST create or update a user profile record in the platform database on every successful authentication or token refresh, keyed to the identity provider's user identifier.
- **FR-004**: The system MUST act as the sole interface to the identity provider. No client application or other service may call the identity provider's authentication endpoints directly.
- **FR-005**: The system MUST return standardised error responses for each failure condition:
  - Invalid credentials: 401 with error code `invalid_credentials`
  - Expired or revoked refresh token: 401 with error code `token_expired`
  - Identity provider unreachable: 503 with error code `auth_unavailable`
  - Malformed request: 400 with error code `validation_error` and a list of details
- **FR-006**: The system MUST NOT expose identity provider internal endpoints, URLs, or credentials to clients in any response.
- **FR-006a**: The system MUST validate token format (non-empty, valid JWT structure) before contacting the identity provider. Malformed tokens must return 400 `validation_error` without any Keycloak call.
- **FR-007**: The system MUST validate and pass through the token audience claim. It must NOT mint or modify tokens — the identity provider remains the sole token issuer.
- **FR-008**: The system MUST provide a logout endpoint that accepts a refresh token and revokes the session with the identity provider. The endpoint must succeed (no error) even if the token is already expired or revoked. On success, it returns `{"message": "logged_out"}`.
- **FR-009**: The system MUST implement per-IP login rate limiting: 10 attempts/minute per IP. Implemented as Auth Service middleware. The rate limit must not apply to refresh, logout, or /me endpoints.
- **FR-010**: The system MUST provide a `GET /api/v1/auth/me` endpoint that accepts an access token (via `Authorization: Bearer` header), validates it, and returns the synchronized user profile from the platform database. This enables dashboard bootstrapping, role-gating, and mobile auth without frontend JWT decoding.

### Endpoints

| Method | Path | Description | Story |
|--------|------|-------------|-------|
| `POST` | `/api/v1/auth/login` | Authenticate with email+password, return token pair | US1 |
| `POST` | `/api/v1/auth/refresh` | Exchange refresh token for new token pair | US2 |
| `POST` | `/api/v1/auth/logout` | Revoke refresh token session | US4 |
| `GET` | `/api/v1/auth/me` | Return current user profile from database | US5 |

### Key Entities *(include if feature involves data)*

- **User Profile (USR-)**: A record in the platform database representing an authenticated user. Created and updated exclusively by the Auth Service on each login or token refresh. Keyed to the identity provider's user identifier. Contains profile metadata needed by downstream services.
- **Access Token**: A short-lived signed token presented by clients to access protected API resources. Issued by the identity provider, never by the Auth Service.
- **Refresh Token**: A longer-lived token used to obtain new access tokens without re-authentication. Issued and validated by the identity provider.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Login flow completes with P95 latency under 2 seconds under normal network conditions, measured from the client's perspective.
- **SC-002**: Token refresh completes with P95 latency under 1 second, enabling seamless background token rotation without user-visible delay.
- **SC-003**: A single Auth Service instance handles at least 100 concurrent login or refresh requests without degradation in response time or error rate.
- **SC-004**: Zero authentication requests bypass the Auth Service to reach the identity provider directly, verifiable through identity provider access logs.
- **SC-005**: 100% of login attempts with invalid credentials receive a clear 401 error — never a timeout or internal server error.

## Assumptions

- The identity provider (Keycloak) is already deployed and configured with the `bornemap` realm, including the required clients and roles, as established by the infrastructure setup.
- The platform database already has the `users` schema and associated role (`auth_service_role`) created, as established by the infrastructure setup.
- Clients (dashboard, mobile app) handle token storage and inclusion in subsequent API requests per the platform security guidelines (access tokens in memory only, never in localStorage).
- Network connectivity between the Auth Service and Keycloak is reliable under normal conditions; the 503 error path exists for degradation scenarios.
- The refresh token rotation model is handled by the identity provider — the Auth Service simply proxies the response.
- **Idempotency**: Authentication endpoints (login, refresh, logout) do NOT require `Idempotency-Key` headers. These requests are naturally repeatable — duplicate logins produce new token pairs, duplicate logouts are idempotent at the identity provider, and duplicate refreshes with an already-rotated token return an error.
