# Authentication Flow Documentation
## BorneMap Identity & Authorization

**Version:** 1.0  
**Last Updated:** June 2026  
**Status:** Pre-Sprint Design

---

## 1. Overview

BorneMap uses **Keycloak** as the centralized identity provider with a **single realm** (bornemap).

### Key Principles

- ✅ Keycloak is the **exclusive** source of truth for user identity
- ✅ Auth Service is the **only** gateway to Keycloak
- ✅ Traefik validates JWT via JWKS (no runtime Keycloak calls)
- ✅ Tokens stored securely:
  - Web: Memory only (never localStorage)
  - Mobile: Secure storage (iOS Keychain / Android Secure Enclave)

---

## 2. Keycloak Configuration

### 2.1 Realm: `bornemap`

Single realm serving all applications.

```yaml
Realm Configuration:
  realm: bornemap
  enabled: true
  accessTokenLifespan: 300 (5 minutes)
  refreshTokenLifespan: 1800 (30 minutes)
  offlineSessionIdleTimeout: 2592000 (30 days)
  sslRequired: external (allow HTTP for development)
```

### 2.2 Clients

Three Keycloak OAuth2/OIDC clients:

#### Client 1: mobile-driver-app
```yaml
clientId: mobile-driver-app
protocol: openid-connect
publicClient: true (no client secret)
redirectUris:
  - exp://localhost:19000/--/
  - exp://192.168.x.x:19000/--/  (Expo dev tunnels)
postLogoutRedirectUris:
  - exp://localhost:19000/--/
standardFlowEnabled: true
directAccessGrantsEnabled: true
implicitFlowEnabled: false
```

#### Client 2: web-driver-app
```yaml
clientId: web-driver-app
protocol: openid-connect
publicClient: true
redirectUris:
  - http://localhost:3000/callback
  - http://localhost:5173/callback (Vite dev server)
  - https://driver.bornemap.tn/callback (production)
postLogoutRedirectUris:
  - http://localhost:3000/
  - https://driver.bornemap.tn/
standardFlowEnabled: true
implicitFlowEnabled: false
```

#### Client 3: admin-dashboard
```yaml
clientId: admin-dashboard
protocol: openid-connect
publicClient: true
redirectUris:
  - http://localhost:3001/callback
  - http://localhost:5173/callback (Vite dev)
  - https://admin.bornemap.tn/callback (production)
postLogoutRedirectUris:
  - http://localhost:3001/
  - https://admin.bornemap.tn/
standardFlowEnabled: true
```

### 2.3 Roles

Three roles in the realm:

```yaml
Realm Roles:
  - role:driver
    description: "Public and registered drivers"
  
  - role:partner
    description: "Partners/operators managing stations"
  
  - role:admin
    description: "System administrators"

Default Role:
  Default: role:driver (assigned to all new users)
```

### 2.4 User Federation

Not in validation phase (no LDAP/AD integration yet).

---

## 3. Registration Flow

### 3.1 User Registration Sequence

```
┌─────────────────────────────────────────────┐
│ Client (Web/Mobile)                         │
│ User fills registration form                │
└─────────────────────────────────────────────┘
                    │
                    ├─→ Validate locally (Zod)
                    │   (email format, password strength, etc.)
                    │
                    ├─→ POST /api/v1/auth/register
                    │   {
                    │     "username": "alice",
                    │     "email": "alice@example.com",
                    │     "password": "SecureP@ssw0rd"
                    │   }
                    │
                    ↓
┌─────────────────────────────────────────────┐
│ Auth Service (:3000)                        │
│ 1. Validate input (Zod)                     │
│ 2. Check email uniqueness (users schema)    │
│ 3. Hash password                            │
│ 4. Create user in Keycloak                  │
│    POST /admin/realms/bornemap/users        │
│ 5. Assign role:driver by default            │
│ 6. Create USR_* entry in users.user_profiles│
│ 7. Return user_id                           │
└─────────────────────────────────────────────┘
                    │
                    ├─→ 201 Created
                    │   {
                    │     "user_id": "USR-9xQa2Lp0VmZk",
                    │     "username": "alice",
                    │     "email": "alice@example.com"
                    │   }
                    │
                    ↓
┌─────────────────────────────────────────────┐
│ Client                                      │
│ Display success message                     │
│ Redirect to login page                      │
└─────────────────────────────────────────────┘
```

### 3.2 Error Handling

```yaml
400 - Validation Error:
  code: VALIDATION_ERROR
  details:
    field: email
    reason: "Invalid email format"

409 - User Exists:
  code: USER_ALREADY_EXISTS
  message: "Email already registered"

500 - Server Error:
  code: INTERNAL_ERROR
  message: "Failed to create user (retry)"
```

---

## 4. Login Flow (OAuth2 Authorization Code)

### 4.1 Browser-Based Login (Web)

```
Step 1: Client Initiates Login
┌─────────────────────────────────────────────┐
│ web-driver application                      │
│ Click "Login" button                        │
└─────────────────────────────────────────────┘
                    │
                    ├─→ Redirect to Keycloak Authorization Endpoint
                    │   https://keycloak:8080/auth/realms/bornemap/protocol/openid-connect/auth?
                    │     client_id=web-driver-app
                    │     redirect_uri=http://localhost:3000/callback
                    │     response_type=code
                    │     scope=openid profile email
                    │     state=random_state_value
                    │
                    ↓
┌─────────────────────────────────────────────┐
│ Keycloak Login Page                         │
│ User enters: username, password             │
│ (Optional: MFA/2FA if enabled)              │
└─────────────────────────────────────────────┘
                    │
                    ├─→ POST /protocol/openid-connect/token
                    │   (Keycloak validates credentials)
                    │
                    ├─→ Generate authorization code (short-lived)
                    │
                    ├─→ Redirect back to client
                    │   http://localhost:3000/callback?
                    │     code=abc123...
                    │     state=random_state_value
                    │
                    ↓
┌─────────────────────────────────────────────┐
│ Client Callback Handler                     │
│ 1. Verify state (CSRF protection)           │
│ 2. Extract authorization code               │
│ 3. POST /api/v1/auth/token                  │
│    { "code": "abc123..." }                  │
└─────────────────────────────────────────────┘
                    │
                    ├─→ Auth Service
                    │   1. Validate code with Keycloak
                    │   2. Exchange code for tokens
                    │   3. Verify JWT signature
                    │   4. Extract user info
                    │   5. Sync user to users schema
                    │   6. Return tokens to client
                    │
                    ↓
┌─────────────────────────────────────────────┐
│ Response to Client                          │
│ {                                           │
│   "access_token": "eyJhbGc...",            │
│   "refresh_token": "eyJhbGc...",           │
│   "expires_in": 300,                        │
│   "token_type": "Bearer"                    │
│ }                                           │
└─────────────────────────────────────────────┘
                    │
                    ├─→ Client stores in memory (never localStorage)
                    │
                    ├─→ Set Authorization header for future requests
                    │   Authorization: Bearer {access_token}
                    │
                    └─→ Redirect to home page
```

### 4.2 POST /api/v1/auth/token (Token Exchange)

```yaml
Request:
  method: POST
  path: /api/v1/auth/token
  body:
    code: string (from Keycloak callback)
    redirect_uri: string (must match registered URI)

Flow:
  1. Auth Service receives code
  2. POST /auth/realms/bornemap/protocol/openid-connect/token
     {
       "grant_type": "authorization_code",
       "code": "{authorization_code}",
       "client_id": "web-driver-app",
       "redirect_uri": "http://localhost:3000/callback"
     }
  3. Keycloak validates and returns:
     {
       "access_token": "{JWT}",
       "refresh_token": "{refresh_token}",
       "expires_in": 300,
       "token_type": "Bearer"
     }
  4. Auth Service verifies JWT signature (from JWKS cache)
  5. Auth Service syncs user to users.user_profiles
  6. Returns tokens to client

Response (200):
  {
    "access_token": "eyJhbGciOiJSUzI1NiIsInR...",
    "refresh_token": "eyJhbGciOiJIUzI1NiIsIn...",
    "expires_in": 300,
    "token_type": "Bearer"
  }

Errors:
  400: Invalid code / Expired code
  401: Invalid redirect_uri
  500: Keycloak unavailable
```

---

## 5. Authenticated Request Flow

### 5.1 Authorization Header

```
Client Request:
  GET /api/v1/driver/stations
  Authorization: Bearer eyJhbGciOiJSUzI1NiIsInR...
  Accept: application/json
```

### 5.2 Gateway Validation (Traefik)

```
┌─────────────────────────────────────────────┐
│ Traefik (API Gateway)                       │
│ Receive request with JWT                    │
└─────────────────────────────────────────────┘
                    │
                    ├─ Extract JWT from header
                    │
                    ├─ Verify signature using JWKS
                    │  (Cached from Keycloak)
                    │  https://keycloak:8080/auth/realms/bornemap/protocol/openid-connect/certs
                    │
                    ├─ Check expiration (exp claim)
                    │
                    ├─ Decision:
                    │  ├─ Valid & not expired → Forward to service
                    │  └─ Invalid / Expired / Missing → 401 Unauthorized
                    │
                    ├─ Add JWT claims to request context
                    │  (X-Token-Subject, X-Token-Roles, etc.)
                    │
                    ↓
            If Valid:
            Forward to service with context

            If Invalid:
            ┌──────────────────────────────┐
            │ 401 Unauthorized Response    │
            │ {                            │
            │   "error": "UNAUTHORIZED",   │
            │   "message": "Invalid token" │
            │ }                            │
            └──────────────────────────────┘
```

### 5.3 JWT Token Structure

```
Header:
{
  "alg": "RS256",
  "typ": "JWT",
  "kid": "keycloak_key_id"
}

Payload:
{
  "iss": "https://keycloak:8080/auth/realms/bornemap",
  "sub": "8a8a8a8a-1234-1234-1234-123456789012",  (Keycloak user UUID)
  "preferred_username": "alice",
  "email": "alice@example.com",
  "email_verified": true,
  "given_name": "Alice",
  "family_name": "Johnson",
  "realm_access": {
    "roles": [
      "offline_access",
      "role:driver"
    ]
  },
  "client_access": {
    "web-driver-app": {
      "roles": ["driver"]
    }
  },
  "iat": 1623456789,
  "exp": 1623457089,
  "auth_time": 1623456789,
  "jti": "unique_jwt_id"
}

Signature:
(RS256 signed with Keycloak's private key)
```

---

## 6. Refresh Token Flow

### 6.1 Token Expiration & Refresh

```
Timeline:
┌──────────────────────────┬─────────────────┬──────────────┐
│ Token issued             │ 5 minutes        │ 30 minute    │
│ (iat)                    │ (exp)            │ (refresh exp)│
└──────────────────────────┴─────────────────┴──────────────┘

When access_token expires:
┌─────────────────────────────────────────────┐
│ Client attempts request                     │
│ GET /api/v1/driver/stations                 │
│ Authorization: Bearer {expired_token}       │
└─────────────────────────────────────────────┘
                    │
                    ├─→ Traefik rejects (exp check fails)
                    │   401 Unauthorized
                    │
                    ↓
┌─────────────────────────────────────────────┐
│ Client HTTP Interceptor                     │
│ (auto-refresh logic)                        │
│ 1. Check if refresh_token exists            │
│ 2. POST /api/v1/auth/refresh                │
│    { "refresh_token": "..." }               │
└─────────────────────────────────────────────┘
                    │
                    ├─→ Auth Service
                    │   1. Validate refresh_token signature
                    │   2. Check if expired
                    │   3. POST /auth/realms/bornemap/protocol/openid-connect/token
                    │      {
                    │        "grant_type": "refresh_token",
                    │        "refresh_token": "...",
                    │        "client_id": "web-driver-app"
                    │      }
                    │   4. Keycloak returns new access_token
                    │   5. Return new tokens to client
                    │
                    ↓
┌─────────────────────────────────────────────┐
│ Client updates tokens                       │
│ Retry original request with new token       │
└─────────────────────────────────────────────┘
```

### 6.2 POST /api/v1/auth/refresh

```yaml
Request:
  method: POST
  path: /api/v1/auth/refresh
  body:
    refresh_token: string

Response (200):
  {
    "access_token": "new_jwt...",
    "refresh_token": "new_refresh_token...",
    "expires_in": 300,
    "token_type": "Bearer"
  }

Errors:
  401: Invalid or expired refresh_token
  403: Refresh token revoked
  500: Keycloak unavailable
```

---

## 7. Logout Flow

### 7.1 User Initiates Logout

```
┌─────────────────────────────────────────────┐
│ Client (Web/Mobile)                         │
│ User clicks "Logout"                        │
└─────────────────────────────────────────────┘
                    │
                    ├─→ POST /api/v1/auth/logout
                    │   Headers:
                    │   Authorization: Bearer {access_token}
                    │
                    ↓
┌─────────────────────────────────────────────┐
│ Auth Service                                │
│ 1. Extract user_id from JWT                 │
│ 2. (Optional) POST /admin/realms/bornemap/users/{id}/logout
│    (revoke tokens in Keycloak)              │
│ 3. Clear any server-side sessions           │
│ 4. Return 200 OK                            │
└─────────────────────────────────────────────┘
                    │
                    ├─→ 200 OK { "message": "Logout successful" }
                    │
                    ↓
┌─────────────────────────────────────────────┐
│ Client                                      │
│ 1. Clear access_token from memory           │
│ 2. Clear refresh_token                      │
│ 3. Redirect to login page                   │
└─────────────────────────────────────────────┘
```

### 7.2 POST /api/v1/auth/logout

```yaml
Request:
  method: POST
  path: /api/v1/auth/logout
  headers:
    Authorization: "Bearer {token}"

Response (200):
  {
    "message": "Logout successful"
  }

Errors:
  401: Invalid or missing token
  500: Server error
```

---

## 8. Role-Based Access Control (RBAC)

### 8.1 Role Hierarchy

```
┌────────────────────────────────────────────────┐
│ role:admin                                     │
│ → Full system access                           │
│ → All protected endpoints                      │
│ → User management capabilities                 │
└────────────────────────────────────────────────┘

┌────────────────────────────────────────────────┐
│ role:partner                                   │
│ → Can manage own stations/chargers             │
│ → Limited to /api/v1/admin/* for own entities  │
│ → Cannot access other partners' data           │
└────────────────────────────────────────────────┘

┌────────────────────────────────────────────────┐
│ role:driver (default)                          │
│ → Read-only station discovery                  │
│ → Can save favorites                           │
│ → Can submit reviews                           │
│ → Cannot access /api/v1/admin/*                │
└────────────────────────────────────────────────┘
```

### 8.2 Role Assignment

```yaml
At User Creation:
  - All new users assigned role:driver
  - Only admins can assign additional roles

Admin Service Logic:
  @CheckRole("admin")
  POST /api/v1/admin/users/{user_id}/roles
  {
    "role": "role:partner"
  }
```

### 8.3 Role Enforcement (Service-Level)

```rust
// Example: Auth Service
#[post("/api/v1/admin/partners")]
#[require_role("admin")]  // Macro enforces role check
async fn create_partner(
    auth: AuthToken,  // Extracted from JWT
    body: CreatePartnerRequest
) -> Result<PartnerResponse> {
    // Role check happens automatically
    // If user not in role:admin, returns 403 Forbidden
    
    // ... create partner logic
}
```

---

## 9. Security Considerations

### 9.1 Token Storage (Critical)

**Web Applications:**
```javascript
// ✅ CORRECT: Store in memory
let accessToken = null;
let refreshToken = null;

// Response from auth API
({ accessToken, refreshToken } = response.data);

// Add to requests
headers = {
  Authorization: `Bearer ${accessToken}`
};
```

**❌ WRONG:**
```javascript
// NEVER: Store in localStorage
localStorage.setItem('token', token);  // VULNERABLE to XSS

// NEVER: Store in cookies without flags
document.cookie = `token=${token}`;  // Missing HttpOnly, Secure, SameSite
```

**Mobile Applications:**
```swift
// ✅ CORRECT: Keychain (iOS)
let keychain = Keychain(service: "bornemap")
try keychain.set(token, key: "access_token")
let stored = try keychain.getString("access_token")

// ✅ CORRECT: Secure Enclave (iOS 9+)
let query: [String: Any] = [
  kSecClass as String: kSecClassGenericPassword,
  kSecUseDataProtectionKeychain as String: true
]
```

### 9.2 JWT Validation (Traefik)

```yaml
JWKS Cache:
  - Source: https://keycloak:8080/auth/realms/bornemap/protocol/openid-connect/certs
  - Update interval: 1 hour
  - Fail-closed: If JWKS unavailable, reject all requests
  - No fallback to old keys

Signature Verification:
  - Algorithm: RS256 (RSA + SHA256)
  - No HS256 allowed (symmetric key not safe for distributed validation)
```

### 9.3 CORS & CSRF

```yaml
CORS:
  - Allowed origins: https://driver.bornemap.tn, https://admin.bornemap.tn
  - Allowed methods: GET, POST, PUT, DELETE
  - Allowed headers: Authorization, Content-Type
  - Allow credentials: true

CSRF:
  - State parameter (OAuth2 flow)
  - SameSite cookies (if cookies used)
  - CSRF tokens (future, if forms used)
```

### 9.4 Rate Limiting

```yaml
Auth Endpoints:
  /auth/register: 5 requests per hour per IP
  /auth/login: 10 requests per minute per IP (against brute force)
  /auth/refresh: 100 requests per minute per user
  /auth/logout: No limit
```

---

## 10. Error Scenarios

### 10.1 Expired Token

```
Client Request:
  GET /api/v1/driver/stations
  Authorization: Bearer {expired_token}

Traefik Validation:
  - Check exp claim
  - Current time > exp → Token expired

Response (401):
  {
    "error": "UNAUTHORIZED",
    "code": "token_expired",
    "message": "Your session has expired. Please login again."
  }

Client Action:
  - Clear tokens
  - Redirect to login
  - (Or trigger auto-refresh if refresh_token still valid)
```

### 10.2 Invalid Signature

```
Response (401):
  {
    "error": "UNAUTHORIZED",
    "code": "invalid_signature",
    "message": "Token signature verification failed"
  }

Cause:
  - Token tampered with
  - JWKS key rotated (key mismatch)
  - Wrong key used to sign
```

### 10.3 Missing Token

```
Response (401):
  {
    "error": "UNAUTHORIZED",
    "code": "missing_token",
    "message": "Authorization header required"
  }

Solution:
  - Add Authorization header
  - Format: Authorization: Bearer {token}
```

### 10.4 Insufficient Permissions

```
Client Request:
  POST /api/v1/admin/partners
  Authorization: Bearer {driver_token}
  (User has role:driver, not role:admin)

Service Response (403):
  {
    "error": "FORBIDDEN",
    "code": "insufficient_permissions",
    "message": "This operation requires admin role"
  }
```

---

## 11. Integration Points

### 11.1 Frontend Integration

```typescript
// web-driver app
import { useAuthClient } from '@packages/auth-client';

export function LoginPage() {
  const { login, isLoading, error } = useAuthClient();
  
  const handleLogin = async (username: string, password: string) => {
    const result = await login(username, password);
    if (result.success) {
      // Tokens stored in memory by useAuthClient
      navigate('/map');
    }
  };
}
```

### 11.2 API Client Integration

```typescript
// packages/api-client
import { createApiClient } from '@packages/api-client';
import { useAuthClient } from '@packages/auth-client';

const apiClient = createApiClient({
  getAccessToken: () => authClient.getAccessToken(),
  onTokenExpired: () => authClient.refreshToken(),
  onUnauthorized: () => authClient.logout()
});
```

### 11.3 Rust Service Integration

```rust
// Auth Service
use actix_web::{web, HttpRequest, HttpResponse};

#[post("/api/v1/auth/register")]
async fn register(
    body: web::Json<RegisterRequest>,
    db: web::Data<Pool<Postgres>>,
) -> Result<HttpResponse> {
    // Validate input
    body.validate()?;
    
    // Create user in Keycloak
    let keycloak_user = keycloak_client.create_user(
        &body.username,
        &body.email,
        &body.password
    ).await?;
    
    // Sync to database
    sqlx::query(
        "INSERT INTO users.user_profiles (id, keycloak_id, username, email, role) 
         VALUES ($1, $2, $3, $4, $5)"
    )
    .bind(generate_user_id())  // USR-{nanoid(12)}
    .bind(keycloak_user.id)
    .bind(&body.username)
    .bind(&body.email)
    .bind("role:driver")
    .execute(db.get_ref())
    .await?;
    
    Ok(HttpResponse::Created().json(UserResponse { ... }))
}
```

---

## 12. Testing Auth Flows

### 12.1 Unit Tests

```rust
#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_jwt_validation() {
        let token = generate_test_jwt("driver");
        assert!(validate_jwt(&token).is_ok());
        assert_eq!(extract_role(&token), "driver");
    }
    
    #[tokio::test]
    async fn test_expired_token_rejection() {
        let expired = generate_expired_jwt();
        assert!(validate_jwt(&expired).is_err());
    }
}
```

### 12.2 Integration Tests

```typescript
// E2E test
describe('Authentication Flow', () => {
  it('should register and login successfully', async () => {
    // Register
    const registerRes = await fetch('/api/v1/auth/register', {
      method: 'POST',
      body: JSON.stringify({
        username: 'testuser',
        email: 'test@example.com',
        password: 'Test123!@#'
      })
    });
    expect(registerRes.status).toBe(201);
    
    // Login
    const loginRes = await fetch('/api/v1/auth/login', {
      method: 'POST',
      body: JSON.stringify({
        username: 'testuser',
        password: 'Test123!@#'
      })
    });
    expect(loginRes.status).toBe(200);
    const { access_token } = await loginRes.json();
    
    // Use token
    const protectedRes = await fetch('/api/v1/driver/stations', {
      headers: { Authorization: `Bearer ${access_token}` }
    });
    expect(protectedRes.status).toBe(200);
  });
});
```

---

## 13. Configuration & Secrets

### 13.1 Environment Variables

```bash
# Keycloak
KEYCLOAK_URL=https://keycloak.bornemap.tn
KEYCLOAK_REALM=bornemap
KEYCLOAK_CLIENT_ID=auth-service
KEYCLOAK_CLIENT_SECRET=${SECRET_KEYCLOAK_CLIENT_SECRET}

# JWT Validation
JWKS_CACHE_TTL=3600  # 1 hour
JWT_ISSUER=https://keycloak.bornemap.tn/auth/realms/bornemap

# Token Lifetimes
ACCESS_TOKEN_LIFETIME=300     # 5 minutes
REFRESH_TOKEN_LIFETIME=1800   # 30 minutes

# CORS
CORS_ORIGINS=https://driver.bornemap.tn,https://admin.bornemap.tn
```

### 13.2 Secrets (Never in Codebase)

```
✅ Injected via:
  - Environment variables (production)
  - .env.local (development, git-ignored)
  - Kubernetes Secrets (if deployed)

❌ Never:
  - Hardcoded in code
  - Committed to git
  - Logged
  - Exposed in frontend
```

---

**References:**
- See `architecture.md` for service topology
- See `SYSTEM_STATE.md` for current status
- OAuth2/OIDC: https://openid.net/specs/openid-connect-core-1_0.html
- Keycloak: https://www.keycloak.org/docs/latest/
