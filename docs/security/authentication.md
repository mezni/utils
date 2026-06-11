# Authentication & RBAC

**Identity Provider:** Keycloak (self-hosted, internal only)

---

## Architecture

```
Frontend → Backend Service → Keycloak → JWT → Backend → Response
```

**Forbidden:**
- Frontend direct Keycloak calls
- Direct token validation by frontend
- Database access to keycloak_db

---

## Dual Realm Design

### bm-drivers

| Aspect | Value |
|---|---|
| Users | public_driver, registered_driver |
| Auth methods | email/password, Google OAuth |
| Purpose | UX-first discovery app users |

### bm-control

| Aspect | Value |
|---|---|
| Users | partner, admin |
| Auth methods | admin-created accounts, invitation-based |
| Purpose | Operational control plane |

---

## Authentication Flows

### Driver Registration

```
User → Driver Service → Keycloak (bm-drivers) → JWT → users.user_account created
```

### Social Login

```
Google → Keycloak → Driver Service → JWT → platform_db
```

### Partner Invite

```
Admin → Admin Service → Keycloak (bm-control) → Email Invite → Partner Activation
```

---

## RBAC Model

| Role | Realm | Scope |
|---|---|---|
| public_driver | bm-drivers | anonymous discovery |
| registered_driver | bm-drivers | app user features |
| partner | bm-control | station management |
| admin | bm-control | full system control |

### Enforcement

- All authorization enforced at backend service layer only
- Partner scoping: `WHERE partner_id = JWT.partner_id`

### Service Mapping

| Service | Enforced Roles |
|---|---|
| driver-service | public_driver, registered_driver |
| admin-service | partner, admin |
| clickstream-service | all roles (write-only) |

---

## JWT Model

```json
{
  "sub": "USR-abc123def456",
  "realm": "bm-drivers",
  "role": "registered_driver",
  "partner_id": "PRT-abc123def456"
}
```
