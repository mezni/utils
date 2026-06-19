# API Contracts Overview
## OpenAPI-First Design

**Version:** 1.0  
**Last Updated:** June 2026  
**Status:** Contract Design Phase

---

## 1. OpenAPI Structure

All API contracts are defined in:
```
api/openapi/
├── auth.yaml           (Auth Service endpoints)
├── driver.yaml         (Driver Service endpoints)
├── admin.yaml          (Admin Service endpoints)
└── shared.yaml         (Common DTOs & schemas)
```

---

## 2. Gateway Routing (Traefik)

```
Client Request
    │
    ├─→ Traefik (:80, :443)
    │   │
    │   ├─ TLS Termination
    │   ├─ JWT Validation (JWKS cache)
    │   └─ Route Dispatch
    │
    ├─→ /api/v1/auth/*   → Auth Service (:3000)
    ├─→ /api/v1/driver/* → Driver Service (:3001)
    └─→ /api/v1/admin/*  → Admin Service (:3002)
```

---

## 3. Auth Service API (:3000)

### 3.1 Base Path
```
/api/v1/auth
```

### 3.2 Endpoints (Design)

#### POST /register
Create a new user account
```yaml
request:
  body:
    username: string (required, 3-50 chars)
    email: string (required, valid email)
    password: string (required, min 8 chars)

responses:
  201:
    user_id: string (USR-{nanoid(12)})
    username: string
    email: string
  400: ValidationError
  409: UserAlreadyExists
```

#### POST /login
Exchange credentials for JWT token
```yaml
request:
  body:
    username: string
    password: string

responses:
  200:
    access_token: string (JWT)
    refresh_token: string
    expires_in: number (seconds)
    token_type: "Bearer"
  401: InvalidCredentials
  429: TooManyAttempts
```

#### POST /refresh
Refresh expired access token
```yaml
request:
  body:
    refresh_token: string

responses:
  200:
    access_token: string (JWT)
    expires_in: number
  401: InvalidToken
```

#### POST /logout
Revoke tokens (optional)
```yaml
request:
  headers:
    Authorization: "Bearer {token}"

responses:
  200: { message: "Logout successful" }
  401: Unauthorized
```

#### GET /me
Fetch current authenticated user profile
```yaml
request:
  headers:
    Authorization: "Bearer {token}" (required)

responses:
  200:
    user_id: string (USR-*)
    username: string
    email: string
    role: string (driver | partner | admin)
    created_at: ISO8601
  401: Unauthorized
  404: UserNotFound
```

#### PUT /profile
Update user profile
```yaml
request:
  headers:
    Authorization: "Bearer {token}" (required)
  body:
    username: string (optional)
    email: string (optional)
    password: string (optional)

responses:
  200:
    user_id: string
    username: string
    email: string
  400: ValidationError
  401: Unauthorized
  409: DuplicateField
```

---

## 4. Driver Service API (:3001)

### 4.1 Base Path
```
/api/v1/driver
```

### 4.2 Endpoints (Design)

#### GET /stations
List all stations with pagination
```yaml
request:
  query:
    page: number (default: 1)
    limit: number (default: 20, max: 100)
    city: string (optional, filter)
    region: string (optional, filter)

responses:
  200:
    data:
      - station_id: string (STA-*)
        name: string
        partner: string
        location: { lat: number, lon: number }
        charger_count: number
        last_updated: ISO8601
    pagination:
      page: number
      limit: number
      total: number
      pages: number
  400: InvalidQuery
```

#### GET /stations/{station_id}
Fetch detailed station information
```yaml
request:
  params:
    station_id: string (STA-*)

responses:
  200:
    station_id: string
    name: string
    address: string
    city: string
    region: string
    location: { lat: number, lon: number }
    partner_id: string (OPR-*)
    partner_name: string
    chargers:
      - charger_id: string (CHG-*)
        type: string (AC | DC)
        power_kw: number
        availability: string (available | occupied | offline)
    reviews_count: number
    rating: number (0-5)
    created_at: ISO8601
    updated_at: ISO8601
  404: StationNotFound
```

#### GET /search
Spatial search (nearby stations)
```yaml
request:
  query:
    lat: number (required, latitude)
    lon: number (required, longitude)
    radius: number (required, meters, max: 50000)
    type: string (optional, AC | DC)

responses:
  200:
    data:
      - station_id: string
        name: string
        distance_m: number
        location: { lat: number, lon: number }
        charger_count: number
    count: number
  400: InvalidQuery (invalid lat/lon/radius)
  429: TooManyQueries (rate limited)
```

#### GET /chargers/{charger_id}
Fetch charger details
```yaml
request:
  params:
    charger_id: string (CHG-*)

responses:
  200:
    charger_id: string
    station_id: string
    station_name: string
    type: string (AC | DC)
    power_kw: number
    availability: string
    last_status_update: ISO8601
  404: ChargerNotFound
```

#### GET /favorites (auth-required)
List user's saved stations
```yaml
request:
  headers:
    Authorization: "Bearer {token}" (required)
  query:
    page: number (default: 1)
    limit: number (default: 20)

responses:
  200:
    data:
      - station_id: string
        name: string
        saved_at: ISO8601
    pagination: { page, limit, total, pages }
  401: Unauthorized
```

#### POST /favorites (auth-required)
Save a station as favorite
```yaml
request:
  headers:
    Authorization: "Bearer {token}" (required)
  body:
    station_id: string (STA-*)

responses:
  201:
    favorite_id: string
    station_id: string
    saved_at: ISO8601
  400: InvalidStation
  401: Unauthorized
  409: AlreadySaved
```

#### DELETE /favorites/{favorite_id} (auth-required)
Remove a favorite
```yaml
request:
  headers:
    Authorization: "Bearer {token}" (required)
  params:
    favorite_id: string

responses:
  204: NoContent
  401: Unauthorized
  404: FavoriteNotFound
```

#### GET /reviews
List reviews for a station
```yaml
request:
  query:
    station_id: string (STA-*, required)
    page: number (default: 1)
    limit: number (default: 10)

responses:
  200:
    data:
      - review_id: string
        user_id: string (USR-*)
        rating: number (1-5)
        comment: string
        created_at: ISO8601
    pagination: { page, limit, total, pages }
  400: InvalidStation
```

#### POST /reviews (auth-required)
Submit a review
```yaml
request:
  headers:
    Authorization: "Bearer {token}" (required)
  body:
    station_id: string (STA-*)
    rating: number (1-5, required)
    comment: string (optional, max 500 chars)

responses:
  201:
    review_id: string
    user_id: string
    station_id: string
    rating: number
    comment: string
    created_at: ISO8601
  400: ValidationError
  401: Unauthorized
  409: AlreadyReviewed (one review per user per station)
```

---

## 5. Admin Service API (:3002)

### 5.1 Base Path
```
/api/v1/admin
```

### 5.2 Endpoints (Design)

#### Partners CRUD

##### POST /partners (admin-required)
Create a new partner/operator
```yaml
request:
  headers:
    Authorization: "Bearer {token}" (required, role: admin)
  body:
    name: string (required, 2-100 chars)
    slug: string (required, unique, 2-50 chars, lowercase)
    contact_email: string (required, valid email)

responses:
  201:
    partner_id: string (OPR-{nanoid(12)})
    name: string
    slug: string
    contact_email: string
    created_at: ISO8601
  400: ValidationError
  401: Unauthorized
  403: Forbidden (not admin)
  409: DuplicateSlug
```

##### GET /partners (admin-required)
List all partners
```yaml
request:
  headers:
    Authorization: "Bearer {token}" (required)
  query:
    page: number (default: 1)
    limit: number (default: 20)
    search: string (optional, search by name/slug)

responses:
  200:
    data:
      - partner_id: string
        name: string
        slug: string
        contact_email: string
        station_count: number
        created_at: ISO8601
    pagination: { page, limit, total, pages }
  401: Unauthorized
```

##### GET /partners/{partner_id} (admin-required)
Fetch partner details
```yaml
request:
  headers:
    Authorization: "Bearer {token}" (required)
  params:
    partner_id: string (OPR-*)

responses:
  200:
    partner_id: string
    name: string
    slug: string
    contact_email: string
    station_count: number
    created_at: ISO8601
    updated_at: ISO8601
  401: Unauthorized
  404: PartnerNotFound
```

##### PUT /partners/{partner_id} (admin-required)
Update partner
```yaml
request:
  headers:
    Authorization: "Bearer {token}" (required)
  params:
    partner_id: string (OPR-*)
  body:
    name: string (optional)
    contact_email: string (optional)

responses:
  200:
    partner_id: string
    name: string
    contact_email: string
    updated_at: ISO8601
  400: ValidationError
  401: Unauthorized
  404: PartnerNotFound
```

##### DELETE /partners/{partner_id} (admin-required)
Delete partner (cascade to stations/chargers)
```yaml
request:
  headers:
    Authorization: "Bearer {token}" (required)
  params:
    partner_id: string (OPR-*)

responses:
  204: NoContent
  401: Unauthorized
  404: PartnerNotFound
  409: Conflict (has dependent stations)
```

#### Stations CRUD

##### POST /stations (admin-required)
Create a new station
```yaml
request:
  headers:
    Authorization: "Bearer {token}" (required, role: admin)
  body:
    partner_id: string (OPR-*, required)
    name: string (required)
    address: string (required)
    city: string (required)
    region: string (required)
    location:
      lat: number (required, -90 to 90)
      lon: number (required, -180 to 180)

responses:
  201:
    station_id: string (STA-{nanoid(12)})
    partner_id: string
    name: string
    location: { lat, lon }
    created_at: ISO8601
  400: ValidationError
  401: Unauthorized
  403: Forbidden (not admin)
```

##### GET /stations (admin-required)
List all stations
```yaml
request:
  headers:
    Authorization: "Bearer {token}" (required)
  query:
    page: number (default: 1)
    limit: number (default: 20)
    partner_id: string (optional, filter by partner)

responses:
  200:
    data:
      - station_id: string
        partner_id: string
        name: string
        city: string
        charger_count: number
        created_at: ISO8601
    pagination: { page, limit, total, pages }
  401: Unauthorized
```

##### PUT /stations/{station_id} (admin-required)
Update station
```yaml
request:
  headers:
    Authorization: "Bearer {token}" (required)
  params:
    station_id: string (STA-*)
  body:
    name: string (optional)
    address: string (optional)
    city: string (optional)
    location: { lat, lon } (optional)

responses:
  200:
    station_id: string
    name: string
    updated_at: ISO8601
  400: ValidationError
  401: Unauthorized
  404: StationNotFound
```

##### DELETE /stations/{station_id} (admin-required)
Delete station (cascade to chargers)
```yaml
request:
  headers:
    Authorization: "Bearer {token}" (required)
  params:
    station_id: string (STA-*)

responses:
  204: NoContent
  401: Unauthorized
  404: StationNotFound
```

#### Chargers CRUD

##### POST /chargers (admin-required)
Create a new charger
```yaml
request:
  headers:
    Authorization: "Bearer {token}" (required)
  body:
    station_id: string (STA-*, required)
    type: string (AC | DC, required)
    power_kw: number (required, > 0)

responses:
  201:
    charger_id: string (CHG-{nanoid(12)})
    station_id: string
    type: string
    power_kw: number
    availability: string (available)
    created_at: ISO8601
  400: ValidationError
  401: Unauthorized
  403: Forbidden (not admin)
```

##### PUT /chargers/{charger_id} (admin-required)
Update charger
```yaml
request:
  headers:
    Authorization: "Bearer {token}" (required)
  params:
    charger_id: string (CHG-*)
  body:
    type: string (optional)
    power_kw: number (optional)
    availability: string (optional)

responses:
  200:
    charger_id: string
    updated_at: ISO8601
  400: ValidationError
  401: Unauthorized
  404: ChargerNotFound
```

##### DELETE /chargers/{charger_id} (admin-required)
Delete charger
```yaml
request:
  headers:
    Authorization: "Bearer {token}" (required)
  params:
    charger_id: string (CHG-*)

responses:
  204: NoContent
  401: Unauthorized
  404: ChargerNotFound
```

#### Analytics

##### GET /analytics (admin-required)
Fetch analytical data
```yaml
request:
  headers:
    Authorization: "Bearer {token}" (required)
  query:
    metric: string (required: stations_count | chargers_count | reviews_avg)
    from: ISO8601 (optional, default: 7 days ago)
    to: ISO8601 (optional, default: now)

responses:
  200:
    metric: string
    value: number | array
    period: { from, to }
  400: InvalidQuery
  401: Unauthorized
```

##### GET /audit (admin-required)
Fetch audit logs
```yaml
request:
  headers:
    Authorization: "Bearer {token}" (required)
  query:
    page: number (default: 1)
    limit: number (default: 50)
    entity_type: string (optional: partner | station | charger)
    from: ISO8601 (optional)
    to: ISO8601 (optional)

responses:
  200:
    data:
      - log_id: string
        event_type: string (created | updated | deleted)
        entity_type: string
        entity_id: string
        actor_id: string (USR-*)
        timestamp: ISO8601
        details: object
    pagination: { page, limit, total, pages }
  401: Unauthorized
```

---

## 6. Common Response Schemas (shared.yaml)

### 6.1 Error Response (All Services)

```yaml
ErrorResponse:
  type: object
  properties:
    error:
      type: object
      properties:
        code: string (ERROR_CODE)
        message: string
        details: object (optional)

examples:
  ValidationError:
    error:
      code: "VALIDATION_ERROR"
      message: "Invalid request body"
      details:
        field: "email"
        reason: "Invalid email format"

  Unauthorized:
    error:
      code: "UNAUTHORIZED"
      message: "Missing or invalid authentication token"

  Forbidden:
    error:
      code: "FORBIDDEN"
      message: "Insufficient permissions for this operation"

  NotFound:
    error:
      code: "NOT_FOUND"
      message: "Resource not found"

  Conflict:
    error:
      code: "CONFLICT"
      message: "Resource already exists"

  RateLimited:
    error:
      code: "RATE_LIMITED"
      message: "Too many requests, please retry after 60 seconds"
```

### 6.2 Pagination Schema

```yaml
Pagination:
  type: object
  properties:
    page: number (1-indexed)
    limit: number (items per page)
    total: number (total count)
    pages: number (total pages)
```

### 6.3 Entity ID Format

```yaml
EntityId: string
pattern: "^(USR|OPR|STA|CHG)-[A-Za-z0-9]{12}$"
example: "STA-9xQa2Lp0VmZk"
```

---

## 7. Authentication & Authorization

### 7.1 Token Format

```
Authorization: Bearer eyJhbGciOiJSUzI1NiIsInR...
```

JWT Claims:
```json
{
  "iss": "https://keycloak:8080/auth/realms/bornemap",
  "sub": "keycloak-user-id",
  "preferred_username": "alice",
  "email": "alice@example.com",
  "realm_access": {
    "roles": ["driver", "offline_access"]
  },
  "iat": 1619123456,
  "exp": 1619126456
}
```

### 7.2 Role-Based Access

```
role:driver   → /api/v1/driver/* (read-only)
role:partner  → /api/v1/admin/* (limited, only own stations)
role:admin    → /api/v1/admin/* (full access)
```

---

## 8. Validation Rules (All Services)

### 8.1 String Fields

- No null values unless explicitly optional
- Max length enforced
- Trimmed of whitespace

### 8.2 Numeric Fields

- Must be positive (unless explicitly negative)
- Type coercion not allowed (must be number type)

### 8.3 Entity IDs

- Must follow `<PREFIX>-nanoid(12)` format
- No manual assignment allowed
- Generated server-side only

### 8.4 Geographic Data

- Latitude: -90 to +90
- Longitude: -180 to +180
- Precision: 6+ decimal places (≈0.1m accuracy)

---

## 9. Rate Limiting

All endpoints subject to:
```
- 100 requests per minute per IP (public endpoints)
- 1000 requests per minute per user (authenticated endpoints)
- 10 requests per second per endpoint
```

---

## 10. Versioning

Current API Version: **v1**

Future versions will use:
```
/api/v2/...  (breaking changes only)
/api/v3/...  (etc.)
```

Deprecation policy: Minimum 6 months notice before removal.

---

## 11. OpenAPI Files Location

Each service maintains its own OpenAPI spec:

```
api/openapi/
├── auth.yaml
├── driver.yaml
├── admin.yaml
└── shared.yaml
```

Code generation from OpenAPI:
```bash
npm run generate:api-client  # Frontend client
cargo build                   # Rust types validation
```

---

## 12. Testing Contracts

All contracts tested against:
- **Unit tests:** DTO serialization/deserialization
- **Integration tests:** Full request/response flow
- **E2E tests:** Real client → gateway → service

---

**Next Steps:**
1. Implement OpenAPI specs (YAML)
2. Generate TypeScript client (`api-client` package)
3. Generate Rust types from schemas
4. Implement endpoints per service
5. Test against contracts
