# Backend Architecture

## Version: 1.0
## Status: Active
## Core Philosophy: Documentation is the system. Code is just its execution.

---

## ⚙️ OVERVIEW

BorneMap backend consists of microservices that communicate through defined API endpoints. Each service has clear responsibilities and database ownership rules.

---

## 🏗️ SERVICE STRUCTURE

```
source/services/
├── driver-service/         # MVP-1 only
├── admin-service/          # MVP-2+
└── auth-service/           # MVP-3+
```

---

## 🚦 SERVICE SEQUENCE

**MVP-1: Driver Service First**
1. Implement driver-service
2. Focus on station discovery
3. Integrate with GIS data
4. Add basic analytics

**MVP-2: Admin Service**
1. Build admin-service
2. Add station management
3. Integrate with users
4. Enable admin workflows

**MVP-3: Auth Service**
1. Implement auth-service
2. Integrate with Keycloak
3. Add authentication flows
4. Enable user management

---

## 📝 DRIVER SERVICE

**Scope:** MVP-1 Only

**Port:** 3000

**Purpose:** Station discovery and basic operations

**Responsibilities:**
- Station CRUD operations
- Nearby station search
- GIS data integration (read-only)
- Basic analytics events
- Station status management

**Allowed APIs:**
- GET /api/v1/stations
- GET /api/v1/stations/nearby
- GET /api/v1/stations/{id}

**Database Access:**
- platform_db.inventory (read/write)
- platform_db.gis (read-only)
- analytics_db (append-only)

**Constraints:**
- No authentication required (public endpoints)
- No user management
- No admin features
- Performance optimized for reading

**Tech Stack:**
- Node.js/TypeScript
- Express or Fastify
- PostgreSQL + PostGIS
- Redis (caching)

**Key Components:**
- Station service
- GIS service
- Analytics service
- API routes
- Database connections
- Error handling

---

## 🛠️ ADMIN SERVICE

**Scope:** MVP-2+

**Port:** 3001

**Purpose:** Station and user management

**Responsibilities:**
- Station management (CRUD)
- User management
- Partner management
- Operational workflows
- Admin analytics

**Allowed APIs:**
- POST /api/v1/stations
- PUT /api/v1/stations/{id}
- DELETE /api/v1/stations/{id}
- POST /api/v1/stations/{id}/status
- GET /api/v1/admin/users
- POST /api/v1/admin/users
- GET /api/v1/admin/partners
- All /api/v1/admin/* endpoints

**Database Access:**
- platform_db.inventory (read/write)
- platform_db.users (read/write)
- platform_db.analytics (append-only)

**Constraints:**
- Authentication required
- Authorization checks on all operations
- No admin features outside MVP-2 scope
- Data validation required

**Tech Stack:**
- Node.js/TypeScript
- Express or Fastify
- PostgreSQL + PostGIS
- Redis (caching)
- Zod (validation)

**Key Components:**
- Station service
- User service
- Partner service
- Admin analytics
- API routes
- Database connections
- Error handling
- Authorization middleware

---

## 🔐 AUTH SERVICE

**Scope:** MVP-3+

**Port:** 3002

**Purpose:** Authentication and authorization gateway

**Responsibilities:**
- Keycloak proxy
- JWT token validation
- User authentication
- Authorization checks
- Session management

**Allowed APIs:**
- POST /api/v1/auth/register
- POST /api/v1/auth/login
- POST /api/v1/auth/logout
- POST /api/v1/auth/refresh
- GET /api/v1/auth/me
- POST /api/v1/auth/validate

**Database Access:**
- keycloak_db (internal only)
- platform_db.users (read-only for user metadata)

**Constraints:**
- Only gateway to Keycloak
- No direct database access
- No authentication bypass allowed
- JWT validation on all routes

**Tech Stack:**
- Node.js/TypeScript
- Express or Fastify
- Keycloak client
- JWT validation
- Session management
- Zod (validation)

**Key Components:**
- Keycloak proxy
- JWT validator
- User service
- API routes
- Error handling
- Rate limiting

---

## 🗄️ DATABASE LAYER

### platform_db (PostgreSQL + PostGIS)

**Purpose:** System of record

**Schemas:**

#### inventory schema
- **Owner:** admin-service (write), driver-service (read)
- **Contents:**
  - Stations table
  - Chargers table
  - Station relationships
  - Operational data
- **Access:**
  - Write: admin-service
  - Read: driver-service, admin-service
- **Constraints:** No deletions from gis schema

#### gis schema
- **Owner:** Read-only
- **Contents:**
  - Station locations (PostGIS geometry)
  - Map features
  - Geographic data
- **Access:** Read-only (all services)
- **Constraints:** NO modifications allowed
- **Architectural Rule:** Never delete from gis schema

#### users schema
- **Owner:** auth-service (metadata), admin-service (read)
- **Contents:**
  - User profiles
  - Authentication data
  - User preferences
- **Access:**
  - Read: admin-service, auth-service
  - Write: auth-service (for auth operations)
- **Constraints:** No direct access from driver-service

---

### analytics_db

**Purpose:** Append-only analytics storage

**Contents:**
- Event tracking data
- User behavior
- Station usage
- System performance
- Error logs

**Access:** Write-only (all services)
**Constraints:**
- Append-only, no deletions
- No modifications to existing data
- Must be efficient for high-volume inserts
- Archival strategy required

**Architectural Rule:** Never delete from this database

---

### keycloak_db

**Purpose:** Internal authentication database

**Access:** Internal only
**Constraints:**
- No external access
- No direct database access
- Only auth-service can interact
- Keycloak manages internally

---

## 🔗 API LAYER

### Versioning
- All endpoints MUST follow `/api/v1/*` pattern
- No unversioned routes allowed
- Version changes require migration plan
- API version is part of the path

### Route Structure

```
/api/v1/
├── stations/           # driver-service (MVP-1)
│   ├── GET /          # List stations
│   ├── GET /nearby    # Nearby search
│   └── GET /{id}      # Station details
├── admin/              # admin-service (MVP-2+)
│   ├── stations/      # Station management
│   ├── users/         # User management
│   └── partners/      # Partner management
└── auth/               # auth-service (MVP-3+)
    ├── POST /register # User registration
    ├── POST /login    # User login
    ├── POST /logout   # User logout
    ├── POST /refresh  # Token refresh
    └── GET /me        # Current user
```

### Authentication

**MVP-1:** No authentication required (public endpoints)

**MVP-2+:** All endpoints require JWT authentication

**MVP-3+:** Complete authentication flow with Keycloak

---

## 🔄 COMMUNICATION

### Service Communication
- Services communicate only through API endpoints
- No direct database access between services
- No shared memory or processes
- Synchronous API calls preferred
- API versioning ensures compatibility

### Database Communication
- Each service owns its data models
- Services access only their designated databases
- No shared database access patterns
- Data validation before database operations

---

## 🛡️ SECURITY

### Authentication

**MVP-1:** No authentication (public API)

**MVP-2:** Basic authentication required

**MVP-3+:** Keycloak integration with JWT tokens

### Authorization

- Role-based access control (RBAC)
- Permission checks on all protected endpoints
- Authorization middleware required
- No authorization bypass allowed

### Data Validation

- All inputs validated before processing
- SQL injection prevention (parameterized queries)
- XSS prevention (input sanitization)
- CSRF protection on forms

### Database Security

- Connection pooling
- Connection limits
- Timeout handling
- Encryption in transit (TLS)
- No hardcoded credentials

---

## ⚡ PERFORMANCE

### Database Optimization

- Indexes on frequently queried columns
- Efficient queries (avoid N+1 problems)
- Connection pooling
- Query caching

### API Optimization

- Response size optimization
- Pagination for large datasets
- Caching where appropriate
- Async processing for heavy operations

### Caching

- Redis for frequently accessed data
- Cache invalidation strategies
- Cache warming for performance
- Cache headers on responses

---

## 🧪 TESTING

### Testing Framework
- Unit tests for services
- Integration tests for API
- E2E tests for critical flows
- Database tests

### Coverage Requirements
- Core functionality ≥ 80%
- API endpoints ≥ 90%
- Database operations ≥ 85%
- Business logic ≥ 80%

### Test Infrastructure
- Test database setup
- Mock services
- API testing tools
- Performance testing

---

## 🔄 DOCUMENTATION IS SYSTEM

**Backend architecture rules are documented here.**
**Code must implement documented architecture.**
**Documentation must be updated with changes.**

**Documentation is the system. Code is just its execution.**
