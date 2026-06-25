# Sprint 09 — auth-service OIDC Broker (Single Identity Gateway)

**Constitution:** BorneMap v1.15.2
**Scope:** auth-service (3000) only
**Identity Provider:** Keycloak (fully encapsulated)
**Event System:** FROZEN

## Sprint Goal

Implement a deterministic authentication system where:
- auth-service is the only OIDC broker and identity gateway
- auth-service is the only system allowed to communicate with Keycloak
- frontend is pure transport/UI only (no identity logic)
- sessions are fully managed and owned by auth-service
- Keycloak is fully invisible to all systems except auth-service

## Canonical Auth Flows

### Login Flow
Frontend → POST /auth/login/init → auth-service generates Keycloak URL → Frontend redirects browser → Keycloak authenticates → Keycloak redirects back → Frontend → POST /auth/callback (code) → auth-service exchanges code → creates session → returns session

### Registration Flow
Same as login but /auth/register/init requests Keycloak registration

### Session Retrieval
GET /auth/me → returns session state from auth-service

## API Surface
- POST /auth/login/init → { "redirect_url": "https://..." }
- POST /auth/register/init → { "redirect_url": "https://..." }
- POST /auth/callback → { "code": "..." } → { "session_id", "access_token", "user" }
- GET /auth/me → session state

## Constraints
- Only auth-service communicates with Keycloak
- No frontend OIDC logic, JWT decoding, role parsing
- No service-to-service identity assumptions
- Event system frozen (no usage)
- Only users.user_profiles modified
