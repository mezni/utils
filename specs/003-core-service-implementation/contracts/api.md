# Contract: Core Service API

**Path**: `services/core-service/src/handlers/`
**Consumers**: frontend applications, other services (geo, analytics), external integrations
**Source**: spec FR-002, FR-003, FR-005, FR-006, FR-007, FR-010

## API Base URL

All core-service endpoints are accessible via:
```
https://api.bornemap.tn/api/core/v1/
```

## Authentication

All API endpoints (except health checks) require authentication:
- **Method**: JWT Bearer token in Authorization header
- **Header**: `Authorization: Bearer <jwt-token>`
- **Validation**: Token is validated at both gateway and service level
- **Permissions**: Role-based access control applied to sensitive operations

## Error Response Format

All errors follow RFC 7807 Problem Details format:

```json
{
  "type": "https://api.bornemap.tn/errors/validation-error",
  "title": "Validation Error",
  "status": 400,
  "detail": "One or more validation errors occurred",
  "instance": "/api/core/v1/companies",
  "errors": [
    {
      "field": "name",
      "message": "Name is required"
    }
  ]
}
```

## Common HTTP Status Codes

| Code | Description | Usage |
|------|-------------|-------|
| 200 | OK | Successful GET, PUT |
| 201 | Created | Successful POST |
| 204 | No Content | Successful DELETE |
| 400 | Bad Request | Validation errors, invalid input |
| 401 | Unauthorized | Missing or invalid JWT |
| 403 | Forbidden | Insufficient permissions |
| 404 | Not Found | Resource not found |
| 409 | Conflict | Concurrent modification, version mismatch |
| 422 | Unprocessable Entity | Business logic validation |
| 429 | Too Many Requests | Rate limiting exceeded |
| 500 | Internal Server Error | Unexpected server error |
| 503 | Service Unavailable | Database or dependency unavailable |

## Rate Limiting

- **Default**: 100 requests per minute per authenticated user
- **Burst**: 200 requests per minute
- **Headers**: `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`
- **Response**: HTTP 429 with retry time when exceeded

## API Endpoints

### Health Check

Public endpoint for monitoring service health.

**GET** `/health/core-service`

**Response**:
```json
{
  "status": "healthy",
  "timestamp": "2026-05-23T10:00:00Z",
  "version": "1.0.0",
  "database": "healthy",
  "details": {
    "database": {
      "status": "healthy",
      "response_time_ms": 12
    }
  }
}
```

### Metrics

Public endpoint for Prometheus metrics.

**GET** `/metrics/core-service`

**Response**: Prometheus-compatible metrics text format

### Companies API

#### Create Company

**POST** `/api/core/v1/companies`

**Permissions**: `admin` role required

**Request Body**:
```json
{
  "name": "Tunisia EV Charging",
  "description": "Leading EV charging network in Tunisia",
  "email": "contact@tunisiaev.tn",
  "phone": "+216-71-123-456",
  "website": "https://tunisiaev.tn",
  "address": "123 Avenue Habib Bourguiba, Tunis, Tunisia",
  "logo_url": "https://tunisiaev.tn/logo.png"
}
```

**Response**: 201 Created
```json
{
  "id": "CMP-abc123def",
  "name": "Tunisia EV Charging",
  "description": "Leading EV charging network in Tunisia",
  "email": "contact@tunisiaev.tn",
  "phone": "+216-71-123-456",
  "website": "https://tunisiaev.tn",
  "address": "123 Avenue Habib Bourguiba, Tunis, Tunisia",
  "logo_url": "https://tunisiaev.tn/logo.png",
  "is_active": true,
  "created_at": "2026-05-23T10:00:00Z",
  "updated_at": "2026-05-23T10:00:00Z"
}
```

#### Get Company

**GET** `/api/core/v1/companies/{id}`

**Permissions**: Any authenticated user

**Response**: 200 OK
```json
{
  "id": "CMP-abc123def",
  "name": "Tunisia EV Charging",
  "description": "Leading EV charging network in Tunisia",
  "email": "contact@tunisiaev.tn",
  "phone": "+216-71-123-456",
  "website": "https://tunisiaev.tn",
  "address": "123 Avenue Habib Bourguiba, Tunis, Tunisia",
  "logo_url": "https://tunisiaev.tn/logo.png",
  "is_active": true,
  "created_at": "2026-05-23T10:00:00Z",
  "updated_at": "2026-05-23T10:00:00Z"
}
```

#### Update Company

**PUT** `/api/core/v1/companies/{id}`

**Permissions**: `admin` role required

**Request Headers**:
- `If-Match`: Required for optimistic concurrency (ETag of current version)

**Request Body**:
```json
{
  "name": "Tunisia EV Charging Network",
  "description": "Updated description",
  "email": "updated@tunisiaev.tn",
  "phone": "+216-71-123-456",
  "website": "https://tunisiaev.tn",
  "address": "123 Avenue Habib Bourguiba, Tunis, Tunisia",
  "logo_url": "https://tunisiaev.tn/logo.png"
}
```

**Response**: 200 OK
```json
{
  "id": "CMP-abc123def",
  "name": "Tunisia EV Charging Network",
  "description": "Updated description",
  "email": "updated@tunisiaev.tn",
  "phone": "+216-71-123-456",
  "website": "https://tunisiaev.tn",
  "address": "123 Avenue Habib Bourguiba, Tunis, Tunisia",
  "logo_url": "https://tunisiaev.tn/logo.png",
  "is_active": true,
  "created_at": "2026-05-23T10:00:00Z",
  "updated_at": "2026-05-23T10:01:00Z",
  "version": 2
}
```

#### Delete Company

**DELETE** `/api/core/v1/companies/{id}`

**Permissions**: `admin` role required

**Request Headers**:
- `If-Match`: Required for optimistic concurrency (ETag of current version)

**Response**: 204 No Content

#### List Companies

**GET** `/api/core/v1/companies`

**Permissions**: Any authenticated user

**Query Parameters**:
- `page`: Page number (default: 1)
- `limit`: Items per page (default: 20, max: 100)
- `sort`: Sort field (default: created_at)
- `order`: Sort order (asc/desc, default: desc)
- `search`: Search in name and description
- `is_active`: Filter by active status (true/false)

**Response**: 200 OK
```json
{
  "data": [
    {
      "id": "CMP-abc123def",
      "name": "Tunisia EV Charging",
      "description": "Leading EV charging network in Tunisia",
      "email": "contact@tunisiaev.tn",
      "is_active": true,
      "created_at": "2026-05-23T10:00:00Z",
      "updated_at": "2026-05-23T10:00:00Z"
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 20,
    "total": 1,
    "pages": 1
  }
}
```

### Stations API

#### Create Station

**POST** `/api/core/v1/stations`

**Permissions**: `admin` or `operator` role required

**Request Body**:
```json
{
  "company_id": "CMP-abc123def",
  "name": "Tunis Mall Charging Station",
  "description": "Fast charging station at Tunis Mall",
  "address": "Tunis Mall, Avenue Habib Bourguiba, Tunis, Tunisia",
  "latitude": 36.8065,
  "longitude": 10.1815,
  "phone": "+216-71-123-456",
  "email": "tunismall@tunisiaev.tn",
  "website": "https://tunisiamall.tn",
  "access_type": "public",
  "operating_hours": {
    "monday": "08:00-22:00",
    "tuesday": "08:00-22:00",
    "wednesday": "08:00-22:00",
    "thursday": "08:00-22:00",
    "friday": "08:00-22:00",
    "saturday": "09:00-20:00",
    "sunday": "09:00-20:00"
  },
  "amenities": ["restroom", "cafe", "wifi", "parking"]
}
```

**Response**: 201 Created
```json
{
  "id": "STA-def456ghi",
  "company_id": "CMP-abc123def",
  "name": "Tunis Mall Charging Station",
  "description": "Fast charging station at Tunis Mall",
  "address": "Tunis Mall, Avenue Habib Bourguiba, Tunis, Tunisia",
  "latitude": 36.8065,
  "longitude": 10.1815,
  "phone": "+216-71-123-456",
  "email": "tunismall@tunisiaev.tn",
  "website": "https://tunisiamall.tn",
  "access_type": "public",
  "operating_hours": {
    "monday": "08:00-22:00",
    "tuesday": "08:00-22:00",
    "wednesday": "08:00-22:00",
    "thursday": "08:00-22:00",
    "friday": "08:00-22:00",
    "saturday": "09:00-20:00",
    "sunday": "09:00-20:00"
  },
  "amenities": ["restroom", "cafe", "wifi", "parking"],
  "is_active": true,
  "created_at": "2026-05-23T10:00:00Z",
  "updated_at": "2026-05-23T10:00:00Z"
}
```

#### Get Station

**GET** `/api/core/v1/stations/{id}`

**Permissions**: Any authenticated user

**Response**: 200 OK
```json
{
  "id": "STA-def456ghi",
  "company_id": "CMP-abc123def",
  "name": "Tunis Mall Charging Station",
  "description": "Fast charging station at Tunis Mall",
  "address": "Tunis Mall, Avenue Habib Bourguiba, Tunis, Tunisia",
  "latitude": 36.8065,
  "longitude": 10.1815,
  "phone": "+216-71-123-456",
  "email": "tunismall@tunisiaev.tn",
  "website": "https://tunisiamall.tn",
  "access_type": "public",
  "operating_hours": {
    "monday": "08:00-22:00",
    "tuesday": "08:00-22:00",
    "wednesday": "08:00-22:00",
    "thursday": "08:00-22:00",
    "friday": "08:00-22:00",
    "saturday": "09:00-20:00",
    "sunday": "09:00-20:00"
  },
  "amenities": ["restroom", "cafe", "wifi", "parking"],
  "is_active": true,
  "created_at": "2026-05-23T10:00:00Z",
  "updated_at": "2026-05-23T10:00:00Z"
}
```

#### Update Station

**PUT** `/api/core/v1/stations/{id}`

**Permissions**: `admin` or `operator` role required

**Request Headers**:
- `If-Match`: Required for optimistic concurrency (ETag of current version)

**Request Body**:
```json
{
  "name": "Tunis Mall Premium Charging Station",
  "description": "Updated description",
  "address": "Tunis Mall, Avenue Habib Bourguiba, Tunis, Tunisia",
  "latitude": 36.8065,
  "longitude": 10.1815,
  "phone": "+216-71-123-456",
  "email": "tunismall@tunisiaev.tn",
  "website": "https://tunisiamall.tn",
  "access_type": "public",
  "operating_hours": {
    "monday": "08:00-22:00",
    "tuesday": "08:00-22:00",
    "wednesday": "08:00-22:00",
    "thursday": "08:00-22:00",
    "friday": "08:00-22:00",
    "saturday": "09:00-20:00",
    "sunday": "09:00-20:00"
  },
  "amenities": ["restroom", "cafe", "wifi", "parking", "restaurant"]
}
```

**Response**: 200 OK
```json
{
  "id": "STA-def456ghi",
  "company_id": "CMP-abc123def",
  "name": "Tunis Mall Premium Charging Station",
  "description": "Updated description",
  "address": "Tunis Mall, Avenue Habib Bourguiba, Tunis, Tunisia",
  "latitude": 36.8065,
  "longitude": 10.1815,
  "phone": "+216-71-123-456",
  "email": "tunismall@tunisiaev.tn",
  "website": "https://tunisiamall.tn",
  "access_type": "public",
  "operating_hours": {
    "monday": "08:00-22:00",
    "tuesday": "08:00-22:00",
    "wednesday": "08:00-22:00",
    "thursday": "08:00-22:00",
    "friday": "08:00-22:00",
    "saturday": "09:00-20:00",
    "sunday": "09:00-20:00"
  },
  "amenities": ["restroom", "cafe", "wifi", "parking", "restaurant"],
  "is_active": true,
  "created_at": "2026-05-23T10:00:00Z",
  "updated_at": "2026-05-23T10:01:00Z"
}
```

#### Delete Station

**DELETE** `/api/core/v1/stations/{id}`

**Permissions**: `admin` or `operator` role required

**Request Headers**:
- `If-Match`: Required for optimistic concurrency (ETag of current version)

**Response**: 204 No Content

#### List Stations

**GET** `/api/core/v1/stations`

**Permissions**: Any authenticated user

**Query Parameters**:
- `page`: Page number (default: 1)
- `limit`: Items per page (default: 20, max: 100)
- `sort`: Sort field (default: created_at)
- `order`: Sort order (asc/desc, default: desc)
- `search`: Search in name and description
- `company_id`: Filter by company ID
- `access_type`: Filter by access type
- `is_active`: Filter by active status (true/false)
- `latitude` & `longitude`: Center point for geographic search
- `radius`: Search radius in kilometers (requires lat/lng)

**Response**: 200 OK
```json
{
  "data": [
    {
      "id": "STA-def456ghi",
      "company_id": "CMP-abc123def",
      "name": "Tunis Mall Premium Charging Station",
      "description": "Updated description",
      "address": "Tunis Mall, Avenue Habib Bourguiba, Tunis, Tunisia",
      "latitude": 36.8065,
      "longitude": 10.1815,
      "phone": "+216-71-123-456",
      "email": "tunismall@tunisiaev.tn",
      "website": "https://tunisiamall.tn",
      "access_type": "public",
      "is_active": true,
      "created_at": "2026-05-23T10:00:00Z",
      "updated_at": "2026-05-23T10:01:00Z"
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 20,
    "total": 1,
    "pages": 1
  }
}
```

### Chargers API

#### Create Charger

**POST** `/api/core/v1/chargers`

**Permissions**: `admin` or `operator` role required

**Request Body**:
```json
{
  "station_id": "STA-def456ghi",
  "name": "Fast Charger 1",
  "charger_type": "DCFC",
  "power_kw": 150.0,
  "voltage": 400.0,
  "amperage": 375.0,
  "connectors": [
    {
      "type": "CCS2",
      "power_kw": 150.0,
      "status": "available"
    }
  ],
  "status": "available",
  "network_id": "TN-DC-001",
  "last_maintenance_date": "2026-05-01",
  "next_maintenance_date": "2026-11-01"
}
```

**Response**: 201 Created
```json
{
  "id": "CHR-ghi789jkl",
  "station_id": "STA-def456ghi",
  "name": "Fast Charger 1",
  "charger_type": "DCFC",
  "power_kw": 150.0,
  "voltage": 400.0,
  "amperage": 375.0,
  "connectors": [
    {
      "type": "CCS2",
      "power_kw": 150.0,
      "status": "available"
    }
  ],
  "status": "available",
  "network_id": "TN-DC-001",
  "last_maintenance_date": "2026-05-01",
  "next_maintenance_date": "2026-11-01",
  "is_active": true,
  "created_at": "2026-05-23T10:00:00Z",
  "updated_at": "2026-05-23T10:00:00Z",
  "version": 1
}
```

#### Get Charger

**GET** `/api/core/v1/chargers/{id}`

**Permissions**: Any authenticated user

**Response**: 200 OK
```json
{
  "id": "CHR-ghi789jkl",
  "station_id": "STA-def456ghi",
  "name": " Fast Charger 1",
  "charger_type": "DCFC",
  "power_kw": 150.0,
  "voltage": 400.0,
  "amperage": 375.0,
  "connectors": [
    {
      "type": "CCS2",
      "power_kw": 150.0,
      "status": "available"
    }
  ],
  "status": "available",
  "network_id": "TN-DC-001",
  "last_maintenance_date": "2026-05-01",
  "next_maintenance_date": "2026-11-01",
  "is_active": true,
  "created_at": "2026-05-23T10:00:00Z",
  "updated_at": "2026-05-23T10:00:00Z",
  "version": 1
}
```

#### Update Charger

**PUT** `/api/core/v1/chargers/{id}`

**Permissions**: `admin` or `operator` role required

**Request Headers**:
- `If-Match`: Required for optimistic concurrency (ETag of current version)

**Request Body**:
```json
{
  "name": "Fast Charger 1 - Updated",
  "charger_type": "DCFC",
  "power_kw": 150.0,
  "voltage": 400.0,
  "amperage": 375.0,
  "connectors": [
    {
      "type": "CCS2",
      "power_kw": 150.0,
      "status": "available"
    }
  ],
  "status": "available",
  "network_id": "TN-DC-001",
  "last_maintenance_date": "2026-05-01",
  "next_maintenance_date": "2026-11-01"
}
```

**Response**: 200 OK
```json
{
  "id": "CHR-ghi789jkl",
  "station_id": "STA-def456ghi",
  "name": "Fast Charger 1 - Updated",
  "charger_type": "DCFC",
  "power_kw": 150.0,
  "voltage": 400.0,
  "amperage": 375.0,
  "connectors": [
    {
      "type": "CCS2",
      "power_kw": 150.0,
      "status": "available"
    }
  ],
  "status": "available",
  "network_id": "TN-DC-001",
  "last_maintenance_date": "2026-05-01",
  "next_maintenance_date": "2026-11-01",
  "is_active": true,
  "created_at": "2026-05-23T10:00:00Z",
  "updated_at": "2026-05-23T10:01:00Z",
  "version": 2
}
```

#### Delete Charger

**DELETE** `/api/core/v1/chargers/{id}`

**Permissions**: `admin` or `operator` role required

**Request Headers**:
- `If-Match`: Required for optimistic concurrency (ETag of current version)

**Response**: 204 No Content

#### List Chargers

**GET** `/api/core/v1/chargers`

**Permissions**: Any authenticated user

**Query Parameters**:
- `page`: Page number (default: 1)
- `limit`: Items per page (default: 20, max: 100)
- `sort`: Sort field (default: created_at)
- `order`: Sort order (asc/desc, default: desc)
- `search`: Search in name
- `station_id`: Filter by station ID
- `charger_type`: Filter by charger type
- `status`: Filter by status
- `is_active`: Filter by active status (true/false)

**Response**: 200 OK
```json
{
  "data": [
    {
      "id": "CHR-ghi789jkl",
      "station_id": "STA-def456ghi",
      "name": "Fast Charger 1 - Updated",
      "charger_type": "DCFC",
      "power_kw": 150.0,
      "voltage": 400.0,
      "amperage": 375.0,
      "connectors": [
        {
          "type": "CCS2",
          "power_kw": 150.0,
          "status": "available"
        }
      ],
      "status": "available",
      "network_id": "TN-DC-001",
      "last_maintenance_date": "2026-05-01",
      "next_maintenance_date": "2026-11-01",
      "is_active": true,
      "created_at": "2026-05-23T10:00:00Z",
      "updated_at": "2026-05-23T10:01:00Z",
      "version": 2
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 20,
    "total": 1,
    "pages": 1
  }
}
```

### Favorites API

#### Add Favorite

**POST** `/api/core/v1/favorites`

**Permissions**: Any authenticated user

**Request Body**:
```json
{
  "station_id": "STA-def456ghi",
  "note": "My favorite charging spot at the mall"
}
```

**Response**: 201 Created
```json
{
  "id": "FAV-jkl012mno",
  "user_id": "USR-pqr345stu",
  "station_id": "STA-def456ghi",
  "note": "My favorite charging spot at the mall",
  "created_at": "2026-05-23T10:00:00Z",
  "updated_at": "2026-05-23T10:00:00Z"
}
```

#### Get Favorite

**GET** `/api/core/v1/favorites/{id}`

**Permissions**: Owner of favorite or admin

**Response**: 200 OK
```json
{
  "id": "FAV-jkl012mno",
  "user_id": "USR-pqr345stu",
  "station_id": "STA-def456ghi",
  "note": "My favorite charging spot at the mall",
  "created_at": "2026-05-23T10:00:00Z",
  "updated_at": "2026-05-23T10:00:00Z"
}
```

#### Update Favorite

**PUT** `/api/core/v1/favorites/{id}`

**Permissions**: Owner of favorite

**Request Body**:
```json
{
  "note": "Updated note about my favorite charging spot"
}
```

**Response**: 200 OK
```json
{
  "id": "FAV-jkl012mno",
  "user_id": "USR-pqr345stu",
  "station_id": "STA-def456ghi",
  "note": "Updated note about my favorite charging spot",
  "created_at": "2026-05-23T10:00:00Z",
  "updated_at": "2026-05-23T10:01:00Z"
}
```

#### Delete Favorite

**DELETE** `/api/core/v1/favorites/{id}`

**Permissions**: Owner of favorite

**Response**: 204 No Content

#### List Favorites

**GET** `/api/core/v1/favorites`

**Permissions**: Any authenticated user (lists own favorites)

**Query Parameters**:
- `page`: Page number (default: 1)
- `limit`: Items per page (default: 20, max: 100)
- `sort`: Sort field (default: created_at)
- `order`: Sort order (asc/desc, default: desc)

**Response**: 200 OK
```json
{
  "data": [
    {
      "id": "FAV-jkl012mno",
      "user_id": "USR-pqr345stu",
      "station_id": "STA-def456ghi",
      "note": "Updated note about my favorite charging spot",
      "created_at": "2026-05-23T10:00:00Z",
      "updated_at": "2026-05-23T10:01:00Z"
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 20,
    "total": 1,
    "pages": 1
  }
}
```

### Reviews API

#### Create Review

**POST** `/api/core/v1/reviews`

**Permissions**: Any authenticated user

**Request Body**:
```json
{
  "station_id": "STA-def456ghi",
  "rating": 5,
  "title": "Excellent charging experience",
  "comment": "Fast charging, clean facilities, and great amenities. Highly recommended!"
}
```

**Response**: 201 Created
```json
{
  "id": "REV-mno345pqr",
  "user_id": "USR-pqr345stu",
  "station_id": "STA-def456ghi",
  "rating": 5,
  "title": "Excellent charging experience",
  "comment": "Fast charging, clean facilities, and great amenities. Highly recommended!",
  "is_moderated": false,
  "moderation_status": "pending",
  "created_at": "2026-05-23T10:00:00Z",
  "updated_at": "2026-05-23T10:00:00Z"
}
```

#### Get Review

**GET** `/api/core/v1/reviews/{id}`

**Permissions**: Any authenticated user

**Response**: 200 OK
```json
{
  "id": "REV-mno345pqr",
  "user_id": "USR-pqr345stu",
  "station_id": "STA-def456ghi",
  "rating": 5,
  "title": "Excellent charging experience",
  "comment": "Fast charging, clean facilities, and great amenities. Highly recommended!",
  "is_moderated": false,
  "moderation_status": "pending",
  "created_at": "2026-05-23T10:00:00Z",
  "updated_at": "2026-05-23T10:00:00Z"
}
```

#### Update Review

**PUT** `/api/core/v1/reviews/{id}`

**Permissions**: Owner of review (if not moderated) or admin

**Request Body**:
```json
{
  "rating": 4,
  "title": "Very good charging experience",
  "comment": "Fast charging and clean facilities. Great amenities!"
}
```

**Response**: 200 OK
```json
{
  "id": "REV-mno345pqr",
  "user_id": "USR-pqr345stu",
  "station_id": "STA-def456ghi",
  "rating": 4,
  "title": "Very good charging experience",
  "comment": "Fast charging and clean facilities. Great amenities!",
  "is_moderated": false,
  "moderation_status": "pending",
  "created_at": "2026-05-23T10:00:00Z",
  "updated_at": "2026-05-23T10:01:00Z"
}
```

#### Delete Review

**DELETE** `/api/core/v1/reviews/{id}`

**Permissions**: Owner of review or admin

**Response**: 204 No Content

#### Moderate Review

**PUT** `/api/core/v1/reviews/{id}/moderate`

**Permissions**: `admin` or `moderator` role required

**Request Body**:
```json
{
  "moderation_status": "approved"
}
```

**Response**: 200 OK
```json
{
  "id": "REV-mno345pqr",
  "user_id": "USR-pqr345stu",
  "station_id": "STA-def456ghi",
  "rating": 4,
  "title": "Very good charging experience",
  "comment": "Fast charging and clean facilities. Great amenities!",
  "is_moderated": true,
  "moderation_status": "approved",
  "moderated_by": "USR-admin678",
  "moderated_at": "2026-05-23T10:02:00Z",
  "created_at": "2026-05-23T10:00:00Z",
  "updated_at": "2026-05-23T10:02:00Z"
}
```

#### List Reviews

**GET** `/api/core/v1/reviews`

**Permissions**: Any authenticated user

**Query Parameters**:
- `page`: Page number (default: 1)
- `limit`: Items per page (default: 20, max: 100)
- `sort`: Sort field (default: created_at)
- `order`: Sort order (asc/desc, default: desc)
- `station_id`: Filter by station ID
- `user_id`: Filter by user ID (admin only)
- `rating`: Filter by rating (1-5)
- `moderation_status`: Filter by moderation status
- `is_moderated`: Filter by moderation status (true/false)

**Response**: 200 OK
```json
{
  "data": [
    {
      "id": "REV-mno345pqr",
      "user_id": "USR-pqr345stu",
      "station_id": "STA-def456ghi",
      "rating": 4,
      "title": "Very good charging experience",
      "comment": "Fast charging and clean facilities. Great amenities!",
      "is_moderated": true,
      "moderation_status": "approved",
      "created_at": "2026-05-23T10:00:00Z",
      "updated_at": "2026-05-23T10:02:00Z"
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 20,
    "total": 1,
    "pages": 1
  }
}
```

## OpenAPI Documentation

The complete OpenAPI 3.0 specification is available at:
- **JSON**: `/api/core/v1/api-json`
- **Swagger UI**: `/api/core/v1/docs`

The OpenAPI specification includes:
- All endpoints with detailed descriptions
- Request/response schemas
- Authentication requirements
- Error response formats
- Example requests and responses