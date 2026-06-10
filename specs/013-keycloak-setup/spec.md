# Feature Specification: Keycloak Authentication Setup

**Feature Branch**: `013-keycloak-setup`

**Created**: 2026-06-10

**Status**: Draft

**Input**: User description: "Sprint 3.1 — Keycloak Setup: Keycloak runs in Docker Compose, realm configured, social login works, tokens carry correct claims, realm exports and re-imports cleanly."

## Clarifications

### Session 2026-06-10

- Q: What user lifecycle operations are needed? → A: Registration + manual disable via admin console only
- Q: What level of observability for Keycloak? → A: Standard Docker logs (stdout) only
- Q: What login page language(s)? → A: English only for Sprint 3.1

## User Scenarios & Testing

### User Story 1 - Developer starts Keycloak and verifies it runs (Priority: P1)

A developer runs the Docker Compose stack and Keycloak starts, connects to the shared PostgreSQL database, and passes its health check. The admin console is accessible, confirming the service is operational.

**Why this priority**: Everything depends on Keycloak being up and reachable — no other story can be tested without this foundation.

**Independent Test**: Can be tested by running `docker compose up -d keycloak`, waiting for the health check to pass, and verifying `curl http://localhost:8180/realms/ev-platform` returns realm metadata JSON.

**Acceptance Scenarios**:

1. **Given** the Docker Compose stack is started with the keycloak service, **When** the health check completes, **Then** `curl http://localhost:8180/realms/ev-platform` returns a 200 with realm metadata
2. **Given** Keycloak is running, **When** a developer opens `http://localhost:8180` in a browser, **Then** the Keycloak admin console login page is displayed

---

### User Story 2 - Driver registers with email/password and receives a JWT (Priority: P1)

A new driver visits the login page, creates an account with their email and password, and upon completion receives a signed JWT containing their identity and the default `registered_driver` role.

**Why this priority**: Email/password registration is the primary onboarding path — without it, no driver can use the platform.

**Independent Test**: Can be tested by registering a user via the Keycloak REST API (`/realms/ev-platform/clients-registrations` or equivalent public registration endpoint) and verifying the returned JWT contains `sub`, `email`, `realm_access.roles` including `registered_driver`, and a non-expired `exp`.

**Acceptance Scenarios**:

1. **Given** a new user submits a registration with email and password, **When** the registration succeeds, **Then** a JWT is returned containing `sub`, `email`, `realm_access.roles: ["registered_driver"]`, and a valid `exp` claim
2. **Given** a user attempts to register with an already-used email, **When** the registration is submitted, **Then** an error is returned indicating the email is taken

---

### User Story 3 - Driver logs in via Google SSO (Priority: P2)

A driver chooses "Sign in with Google" on the login page, is redirected to Google's consent screen, authorizes the app, and is redirected back with a valid JWT. On first login, the `registered_driver` role is assigned automatically.

**Why this priority**: Social login reduces friction and improves conversion rates for new drivers; it is a key differentiator but not critical for initial launch.

**Independent Test**: Can be tested by initiating a Google IdP login flow against the Keycloak dev instance and verifying a JWT is returned with the `registered_driver` role after first broker login.

**Acceptance Scenarios**:

1. **Given** a new driver selects Google login, **When** they authorize the app on Google's consent screen, **Then** they are redirected back with a JWT containing `sub`, `email`, and `realm_access.roles: ["registered_driver"]`
2. **Given** a returning driver selects Google login, **When** authorization completes, **Then** they receive a JWT with their existing roles preserved

---

### User Story 4 - Admin assigns partner role and partner_id to a user (Priority: P2)

A platform admin assigns the `partner` role to a specific user via the Keycloak admin console and sets the `partner_id` user attribute. Subsequent JWTs issued to that user include the `partner_id` claim.

**Why this priority**: The `partner_id` claim is required by the existing partner-related APIs to identify which partner a user belongs to; without this, partner features cannot function.

**Independent Test**: Can be tested by setting `partner_id` attribute for a user in the admin console, requesting a new token, and verifying the JWT contains `partner_id: "PRT-..."`.

**Acceptance Scenarios**:

1. **Given** an admin sets the `partner_id` attribute on a user with the `partner` role, **When** the user requests a new token, **Then** the JWT contains a `partner_id` claim with the correct value
2. **Given** a user without the `partner` role, **When** they request a token, **Then** the JWT does not contain a `partner_id` claim

---

### User Story 5 - Backend services authenticate via confidential clients (Priority: P2)

The `driver-service` and `admin-service` authenticate to Keycloak using their confidential client credentials (client ID + client secret) to verify incoming JWTs and obtain service account tokens for inter-service communication.

**Why this priority**: Backend services need to validate JWTs and identify the calling user; this is a prerequisite for securing all API endpoints.

**Independent Test**: Can be tested by having each backend service obtain a service account token from Keycloak and use it to call its own health endpoint with bearer auth.

**Acceptance Scenarios**:

1. **Given** a service has valid client credentials, **When** it requests a service account token from Keycloak, **Then** it receives a valid JWT with appropriate audience claims
2. **Given** a service uses an invalid client secret, **When** it requests a token, **Then** Keycloak returns a 401 error

---

### User Story 6 - Admin exports realm config and re-imports cleanly (Priority: P3)

After configuring the realm (roles, clients, IdPs, mappers), an admin runs a command to export the configuration to a file. The export can then be imported into a fresh Keycloak instance and produce identical behavior.

**Why this priority**: Export/import enables version-controlled infrastructure, repeatable deployments, and disaster recovery — important for production but not needed for initial dev setup.

**Independent Test**: Can be tested by exporting the realm, tearing down the Keycloak container with `docker compose down -v`, bringing it back up with the import, and verifying all configurations (roles, clients, IdPs) match the original.

**Acceptance Scenarios**:

1. **Given** a fully configured realm, **When** the admin runs the export command, **Then** a complete `realm-export.json` file is produced containing all roles, clients, IdPs, mappers, and user federation settings
2. **Given** a fresh Keycloak instance with the export file mounted, **When** it starts with `--import-realm`, **Then** all realm configurations are restored without errors

### Edge Cases

- What happens when Keycloak starts before PostgreSQL is ready? (depends_on with health check prevents this)
- How does the system handle expired refresh tokens? (7-day expiry triggers re-login)
- What happens if a social login IdP (Google/Facebook) is unavailable? (email/password login remains available)
- How is the first admin user created if no realm export exists yet? (manual creation via admin console with default admin credentials)
- What happens when realm export is re-imported over an existing realm with changes? (import overwrites; manual changes since export are lost)

## Requirements

### Functional Requirements

- **FR-001**: System MUST start Keycloak as a Docker Compose service with health checks that verify Keycloak is ready to accept requests
- **FR-002**: Keycloak MUST use the shared PostgreSQL database (in the `keycloak` schema) for persistent storage of realm, user, and session data
- **FR-003**: The `ev-platform` realm MUST be auto-imported on startup from a version-controlled realm export file
- **FR-004**: The realm MUST define three roles: `registered_driver` (default on registration), `partner` (manually assigned), and `admin` (manually assigned)
- **FR-005**: The realm MUST support three public clients (`driver-web`, `driver-mobile`, `dashboard`) with PKCE S256 enforced and two confidential clients (`driver-service`, `admin-service`) with service accounts enabled
- **FR-006**: Users MUST be able to register and log in with email and password, receiving a signed JWT
- **FR-007**: Administrators MUST be able to manually disable a user account via the admin console; disabled users MUST be rejected at login
- **FR-008**: Users MUST be able to log in via Google and Facebook SSO, receiving a JWT with the `registered_driver` role on first login
- **FR-009**: JWTs MUST include `sub`, `email`, `realm_access.roles`, and `exp` claims
- **FR-010**: Users with the `partner` role MUST include a `partner_id` claim in their JWT, sourced from the user's `partner_id` attribute in Keycloak
- **FR-011**: Access tokens MUST expire after 15 minutes, refresh tokens after 7 days, and SSO sessions after 7 days
- **FR-012**: The realm configuration MUST be exportable to a JSON file via the Keycloak export command
- **FR-013**: The export file MUST be re-importable into a fresh Keycloak instance with identical behavior

### Key Entities

- **Realm** (`ev-platform`): The top-level Keycloak namespace that contains all roles, clients, users, and identity provider configurations for the platform
- **Role**: A named permission level (`registered_driver`, `partner`, `admin`) assigned to users to control access to features
- **Client**: A registered application (web app, mobile app, backend service) that authenticates via Keycloak; public clients use PKCE, confidential clients use a client secret
- **User Attribute** (`partner_id`): A custom attribute stored on a Keycloak user record that maps to a partner entity in the database
- **Identity Provider**: An external authentication source (Google, Facebook) configured in Keycloak to enable social login
- **JWT Claim** (`partner_id`): A custom claim injected into the access token via a Protocol Mapper when the user has the `partner` role and a `partner_id` attribute set

## Success Criteria

### Measurable Outcomes

- **SC-001**: Developers can start a full Docker Compose stack with Keycloak and have it pass its health check within 120 seconds
- **SC-002**: A new user can register with email/password and receive a valid JWT in under 5 seconds
- **SC-003**: A returning user can log in via Google SSO and receive a valid JWT with the `registered_driver` role in under 10 seconds (including IdP redirect)
- **SC-004**: An admin can assign the `partner` role with `partner_id` and the user's next JWT includes the claim within 1 minute (configuration propagation time)
- **SC-005**: Realm export produces a complete JSON file; importing into a fresh instance restores all 3 roles, 5 clients, 2 IdPs, and the `partner_id` protocol mapper
- **SC-006**: All existing MVP-2 services (postgres, admin-service, driver-service, dashboard, driver-web, driver-mobile) start and pass health checks alongside Keycloak

## Assumptions

- The Keycloak admin console is used for initial realm configuration (first run); subsequent runs use realm import
- Development Google and Facebook credentials are used; production credentials will be configured separately
- The `partner_id` user attribute is set manually by an admin — no automated synchronization from the partner database is included in this sprint
- Keycloak runs in `start-dev` mode for development; production deployment considerations are out of scope
- Social login IdP credentials (Google Cloud Console, Meta Developer Portal) are managed outside this sprint
- The database migration `CREATE SCHEMA IF NOT EXISTS keycloak` runs as part of the standard migration process before Keycloak starts
- Login and registration pages use English only; internationalization is deferred beyond Sprint 3.1
