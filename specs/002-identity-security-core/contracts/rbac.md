# RBAC Contract

**Purpose**: Define role-based access control rules across all three services.

## Roles

| Role | Priority | Description |
|------|----------|-------------|
| driver | 10 | Standard EV driver — access to driver-facing features |
| partner | 20 | Station partner — access to management features |
| admin | 30 | Full system access |

Roles are ranked by priority. Higher-priority roles inherit access from lower-priority roles.

## Route Protection Rules

| Service | Route Pattern | Allowed Roles | Notes |
|---------|---------------|---------------|-------|
| auth-service | GET /health | public | No auth required |
| auth-service | POST /api/v1/auth/login | public | Authentication endpoint |
| auth-service | POST /api/v1/auth/logout | all authenticated | Requires valid JWT |
| auth-service | GET /api/v1/auth/sync | admin | Manual Keycloak sync trigger |
| auth-service | ANY /* | all authenticated | All other routes require valid JWT |
| driver-service | GET /health | public | No auth required |
| driver-service | GET /api/v1/stations | driver, partner, admin | Nearby station search |
| driver-service | GET /api/v1/telemetry/events | auth-service | Internal event ingestion |
| driver-service | ANY /* | driver, partner, admin | All other routes require valid JWT |
| admin-service | GET /health | public | No auth required |
| admin-service | POST /api/v1/stations | partner, admin | Station CRUD |
| admin-service | PUT /api/v1/stations/* | partner, admin | Station update |
| admin-service | DELETE /api/v1/stations/* | admin | Station deletion |
| admin-service | GET /api/v1/partners/* | partner, admin | Partner management |
| admin-service | GET /api/v1/analytics | admin | Analytics dashboard |
| admin-service | ANY /* | partner, admin | All other routes require valid JWT |

## Middleware Contract

Each service MUST implement a JWT validation middleware that:

1. Extracts the `Authorization: Bearer <jwt>` header
2. Validates the JWT signature against Keycloak JWKS
3. Extracts the `realm_access.roles` claim
4. Maps to a BorneMap role
5. Checks role against route's allowed roles
6. Injects user identity (user_uuid, role) into request context
7. Returns 401 for invalid/missing tokens
8. Returns 403 for valid tokens with insufficient role

## Public Routes (No Auth)

Only the following routes are exempt from JWT validation:
- `GET /health` on all services
- `POST /api/v1/auth/login` on auth-service
