# Authentication Flows

## Responsibility Mapping

### Keycloak (`keycloak_db`)

| Owns | Description |
|------|-------------|
| Credentials | Passwords, hash policies, credential rotation |
| Social IdP federation | Google, Facebook identity brokering (via standard OIDC redirect) |
| Sessions | Active user sessions, refresh tokens |
| JWT issuance | Access token generation, signing keys |
| Realm/role definitions | `bornemap-drivers`, `bornemap-staff` realms |
| Invite links | Partner invite workflow |
| Approval states | Partner registration approval status |

**Keycloak IS the source of truth for**: "who is this person", "are they allowed in", "what role do they have"

Managed via: `kcadm` scripts (initial setup) + Keycloak admin API (runtime partner invite/approve).

### `users` Schema (`platform_db`)

| Owns | Description |
|------|-------------|
| App-level profile | `display_name`, `avatar_url`, `created_at`, keyed by `keycloak_id` |

**Created on**: first successful login (post-JWT validation hook in Auth Service).

**Explicit exclusions**: no passwords, no roles, no session data — ever.

**Source of truth for**: "what does this user's BorneMap profile look like"

### Auth Service (`:3000`)

| Responsibility | Detail |
|----------------|--------|
| `users` schema reads/writes | Sole owner of `users.driver_profile` CRUD |
| Create profile on first login | Triggered by authenticated request with no existing profile |
| `GET /api/v1/users/me` | Return profile for authenticated driver |
| Partner invite | `POST /api/v1/admin/partners/invite` — talks to Keycloak admin API |
| Partner approval | `POST /api/v1/admin/partners/{id}/approve` — talks to Keycloak admin API |

**Does NOT**: issue tokens (Keycloak does), store credentials, manage sessions, proxy token refresh.

### Other Services (Driver, Admin, GIS)

- Validate JWTs independently via **shared middleware** (JWKS fetched from Keycloak at startup, cached)
- **Never call Auth Service per request** — only fetch Keycloak JWKS at startup / cache refresh
- **Never touch `users` schema directly** — call Auth Service if user profile data is needed

---

## Partner Status Field

The `inventory.partner` table uses a `partner_status` enum:

| Status | Definition |
|--------|------------|
| `pending` | Partner self-registered, awaiting admin approval; restricted dashboard access |
| `active` | Approved by admin; full access |
| `suspended` | Temporarily disabled by admin; action blocked with `OPR_004` |
| `closed` | Permanently removed (soft delete); filtered from all queries |
| `rejected` | Denied by admin; login returns `AUTH_007` |

Notification of approval/rejection is **deferred** (out of scope for MVP-3). During validation phase, partners must refresh or be notified out-of-band.

---

## Keycloak Setup

| Realm | Audience | Purpose |
|-------|----------|---------|
| `bornemap-drivers` | Drivers (public + registered) | Self-registration, social login, station discovery |
| `bornemap-staff` | Admins + Partners | Station management, dashboard access |

### Realm Configuration (via `kcadm.sh`)

```bash
# Create realms
kcadm.sh create realms -s realm=bornemap-drivers -s enabled=true
kcadm.sh create realms -s realm=bornemap-staff -s enabled=true

# Create clients per realm
kcadm.sh create clients -r bornemap-drivers -s clientId=mobile-app -s publicClient=true
kcadm.sh create clients -r bornemap-drivers -s clientId=web-driver -s publicClient=true
kcadm.sh create clients -r bornemap-staff -s clientId=dashboard -s publicClient=false -s secret=***
```

---

## Flow 1: Public Driver Registration (email/password)

```
[Driver]              [Auth Service]          [Keycloak (drivers realm)]     [platform_db.users]
   │                        │                          │                          │
   │── POST /api/v1/auth/register ──>                   │                          │
   │   {email, password}     │                          │                          │
   │                        │── kcadm create user ──────>                          │
   │                        │                          │── (password set)          │
   │                        │<── keycloak_id (uuid) ────                          │
   │                        │                          │                          │
   │                        │── INSERT driver_profile ────────────────────────────>│
   │                        │   (keycloak_id, email)    │                          │
   │                        │                          │                          │
   │<── 201 {user_id, msg} ─│                          │                          │
```

### Details
1. Auth Service receives email + password
2. Auth Service calls Keycloak admin API to create user in `bornemap-drivers` realm
3. Keycloak sends verification email (configurable; may be skipped for validation phase)
4. Auth Service creates profile row in `users.driver_profile`
5. Returns user ID to client

---

## Flow 2: Driver Login (email/password)

```
[Client]              [Keycloak (drivers realm)]
   │                              │
   │── POST /realms/bornemap-drivers/protocol/openid-connect/token ──>
   │   grant_type=password        │
   │   client_id=mobile-app       │
   │   username={email}           │
   │   password={password}        │
   │                              │
   │<── 200 {access_token,        │
   │        refresh_token,        │
   │        expires_in} ──────────│
```

**Auth Service is not in the critical path.** Clients call Keycloak's OIDC token endpoint directly. Auth Service only learns about the user on the first authenticated request to any service (profile creation trigger).

### Token Validation (subsequent requests)

```
[Client]              [Driver Service]          [Keycloak JWKS]
   │                        │                          │
   │── GET /api/v1/stations ─>                         │
   │   Authorization: Bearer │                          │
   │                        │── Fetch JWKS (cached) ───>│
   │                        │<── public keys ──────────│
   │                        │                          │
   │                        │── Validate JWT:          │
   │                        │   • signature            │
   │                        │   • issuer (realm)       │
   │                        │   • audience (client)    │
   │                        │   • expiry               │
   │                        │   • role/realm claim     │
   │                        │                          │
   │<── 200 / 401 ──────────│                          │
```

JWT validation uses JWKS endpoint (no Keycloak admin API calls per-request). Keys cached with TTL. All services use the same shared middleware — no per-service validation logic.

---

## Flow 3: Social Login (Google/Facebook)

Uses Keycloak's **native IdP redirect** pattern — no custom token exchange.

```
[Client]                          [Keycloak (drivers realm)]     [Google/Facebook]
   │                                        │                          │
   │── GET /realms/bornemap-drivers/broker/google/endpoint ──>         │
   │   (redirect to Google login)            │                          │
   │                                        │── redirect to Google ───>│
   │                                        │                          │
   │<── Google login page ─────────────────────────────────────────────│
   │                                        │                          │
   │── (user authenticates with Google) ──────────────────────────────>│
   │                                        │                          │
   │                                        │<── auth code ────────────│
   │                                        │── exchange code for tokens│
   │                                        │                          │
   │<── Keycloak session established ──────│                          │
   │    (same JWT format as password flow)  │                          │
```

### Details
1. Client redirects user to Keycloak's IdP broker endpoint for the chosen provider
2. Keycloak redirects to Google/Facebook for authentication
3. User authenticates on the provider's domain
4. Provider sends auth code to Keycloak (registered callback)
5. Keycloak exchanges code for tokens, links social identity to local user
6. Keycloak returns JWT to client (same format as password login)

Client apps use Keycloak's OIDC client library (e.g. `@react-keycloak/web` for React, `@react-native-keycloak` for mobile) to manage the redirect flow.

---

## Flow 4: Partner Registration & Approval

### Option A: Admin Invite (primary flow)

```
[Admin]               [Auth Service]          [Keycloak (staff realm)]    [platform_db]
   │                        │                        │                       │
   │── POST /api/v1/admin/partners/invite ──>        │                       │
   │  {name, email, type}  │                        │                       │
   │                        │── INSERT partner ────────────────────────────>│
   │                        │   (status: active)    │                       │
   │                        │                        │                       │
   │                        │── kcadm create user ───>                       │
   │                        │   (staff realm, temp password)                 │
   │                        │                        │                       │
   │<── 201 {partner_id} ──│                        │                       │
```

Auth Service handles the Keycloak admin API call (sole owner per boundaries). Partner is immediately `active` on invite.

### Option B: Partner Self-Registration (requires admin approval)

```
[Partner]             [Auth Service]          [Keycloak (staff realm)]    [platform_db]
   │                        │                        │                       │
   │── POST /api/v1/auth/partner/register ──>        │                       │
   │  {name, email, type, password}        │                        │                       │
   │                        │── kcadm create user ───>                       │
   │                        │                        │                       │
   │                        │── INSERT partner ────────────────────────────>│
   │                        │   (status: pending)    │                       │
   │<── 201 {partner_id} ──│                        │                       │
   │                        │                        │                       │
[Admin reviews]             │                        │                       │
   │── POST /api/v1/admin/partners/{id}/approve ──>  │                       │
   │                        │── UPDATE partner ────────────────────────────>│
   │                        │   (status: active)     │                       │
   │                        │                        │                       │
   │── POST /api/v1/admin/partners/{id}/reject ───>  │                       │
   │                        │── UPDATE partner ────────────────────────────>│
   │                        │   (status: rejected)   │                       │
```

**Notification**: out of scope for MVP-3. Partner must poll or admins notify out-of-band.

---

## Flow 5: Token Refresh

Direct client → Keycloak (standard OIDC). No Auth Service proxy.

```
[Client]                    [Keycloak]
   │                              │
   │── POST /realms/bornemap-drivers/protocol/openid-connect/token ──>
   │   grant_type=refresh_token    │
   │   client_id=mobile-app       │
   │   refresh_token={token}      │
   │                              │
   │<── 200 {new_access_token,    │
   │        new_refresh_token,    │
   │        expires_in} ──────────│
```

Same pattern for `bornemap-staff` realm with `client_id=dashboard`.

---

## Flow 6: Profile Retrieval (`GET /api/v1/users/me`)

Triggered by any authenticated client that needs the user's BorneMap profile. First request also creates the profile if it doesn't exist (lazy creation).

```
[Client]              [Auth Service]          [platform_db.users]
   │                        │                       │
   │── GET /api/v1/users/me ─>                      │
   │   Authorization: Bearer │                       │
   │                        │── SELECT by keycloak_id│
   │                        │   (extracted from JWT) │
   │                        │                       │
   │                        │── (if not found)      │
   │                        │── INSERT driver_profile│
   │                        │   (keycloak_id, email) │
   │                        │                       │
   │<── 200 {user_id,       │                       │
   │        display_name,   │                       │
   │        email,          │                       │
   │        created_at} ────│                       │
```

### Lazy Profile Creation
- Profile row is created on first `GET /api/v1/users/me`, not at registration time
- This decouples Auth Service from Keycloak's registration flow
- Registration creates the Keycloak user; profile is created on first post-login API call
- Works for both email/password and social login (same path)

---

## Service-to-Service Auth (Internal)

During validation phase (MVP-1 through MVP-5): no internal auth. Services trust each other via Docker network. Traefik (MVP-6) adds mTLS between services if required.

---

## Error Codes per Flow

| Code | HTTP Status | Message | When |
|------|-------------|---------|------|
| `AUTH_001` | 409 | Email already registered | Registration with duplicate email |
| `AUTH_002` | 422 | Password does not meet strength requirements | Password < 8 chars, no uppercase, etc. |
| `AUTH_003` | 401 | Invalid access token | JWT expired, bad signature, or malformed |
| `AUTH_004` | 403 | Insufficient permissions | Valid token but wrong realm/role for endpoint |
| `AUTH_005` | 500 | Keycloak communication failed | Auth Service cannot reach Keycloak |
| `AUTH_006` | 403 | Partner account pending approval | Partner tries to use dashboard before admin approves |
| `AUTH_007` | 403 | Partner registration rejected | Partner tries to login after rejection |

Note: login, social login, token refresh, and logout are handled directly by Keycloak OIDC endpoints. Error codes for those flows come from Keycloak directly, not Auth Service. See `docs/spec/error-catalog.md` for the full catalog.
