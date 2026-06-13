# Services Architecture

## Version: 1.0
## Status: Active
## Core Philosophy: Documentation is the system. Code is just its execution.

---

## ⚙️ OVERVIEW

BorneMap services are independently deployable microservices that communicate through defined API contracts. Each service has clear responsibilities and scope boundaries.

---

## 🎯 SERVICE SCOPE

### MVP-1: Driver Service
**Focus:** Station discovery and basic operations

### MVP-2: Admin Service
**Focus:** Station and user management

### MVP-3: Auth Service
**Focus:** Authentication and authorization

---

## 🚀 SERVICE ARCHITECTURE

```
┌─────────────────────────────────────────────────────────────┐
│                         Service Layer                        │
├─────────────────────────────────────────────────────────────┤
│  ⚙️ driver-service :3000       (MVP-1 Only)                 │
│  🛠️ admin-service :3001        (MVP-2+)                     │
│  🔐 auth-service :3002          (MVP-3+)                     │
└─────────────────────────────────────────────────────────────┘
                          │
                          │ API calls (/api/v1/*)
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                      Edge Layer                              │
├─────────────────────────────────────────────────────────────┤
│  🌐 Traefik (API Gateway)                                    │
│  • API routing                                                │
│  • TLS termination                                           │
│  • Load balancing                                            │
│  • Rate limiting                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## 📝 DRIVER SERVICE

### Scope and Responsibilities

**MVP-1 Only**
- Station CRUD operations
- Nearby station search
- GIS data integration (read-only)
- Basic analytics events
- Station status management

**No Access To:**
- User management
- Authentication flows
- Admin features
- Partner operations

### API Endpoints

#### Station Operations
- `GET /api/v1/stations` - List all stations
- `GET /api/v1/stations/nearby` - Search nearby stations
- `GET /api/v1/stations/{id}` - Get station details

#### Features
- Station discovery
- Location-based search
- Station filtering
- Pagination

### Database Access

**Read Access:**
- `platform_db.inventory` - Station data
- `platform_db.gis` - Map data (read-only)

**Write Access:**
- `analytics_db` - Append-only event tracking

### Constraints

- No authentication required (public API)
- No user management
- No admin features
- Performance optimized for reading
- No database modifications for gis schema

### Deployment

- Standalone Node.js service
- Docker containerization
- Health check endpoints
- Graceful shutdown
- Performance monitoring

---

## 🛠️ ADMIN SERVICE

### Scope and Responsibilities

**MVP-2+**
- Station management (CRUD)
- User management
- Partner management
- Operational workflows
- Admin analytics

**No Access To:**
- Authentication gateway (handled by auth-service)
- Future MVP-4+ features
- Non-admin features

### API Endpoints

#### Station Management
- `POST /api/v1/stations` - Create station
- `PUT /api/v1/stations/{id}` - Update station
- `DELETE /api/v1/stations/{id}` - Delete station
- `POST /api/v1/stations/{id}/status` - Update status

#### User Management
- `GET /api/v1/admin/users` - List users
- `POST /api/v1/admin/users` - Create user
- `PUT /api/v1/admin/users/{id}` - Update user
- `DELETE /api/v1/admin/users/{id}` - Delete user

#### Partner Management
- `GET /api/v1/admin/partners` - List partners
- `POST /api/v1/admin/partners` - Create partner
- `PUT /api/v1/admin/partners/{id}` - Update partner
- `DELETE /api/v1/admin/partners/{id}` - Delete partner

#### Admin Features
- Station configuration
- Operational settings
- User roles and permissions
- System status monitoring

### Database Access

**Read Access:**
- `platform_db.inventory` - Station data
- `platform_db.users` - User data
- `platform_db.gis` - Map data (read-only)
- `platform_db.analytics` - Analytics data

**Write Access:**
- `platform_db.inventory` - Station data modifications
- `platform_db.users` - User data modifications
- `platform_db.analytics` - Analytics events

### Constraints

- Authentication required (JWT)
- Authorization checks on all operations
- Data validation required
- Audit logging required
- No admin features outside MVP-2 scope

### Deployment

- Standalone Node.js service
- Docker containerization
- Health check endpoints
- Graceful shutdown
- Performance monitoring
- Audit logging

---

## 🔐 AUTH SERVICE

### Scope and Responsibilities

**MVP-3+**
- Keycloak proxy
- JWT token validation
- User authentication
- Authorization checks
- Session management

**No Access To:**
- Station management (handled by driver-service or admin-service)
- User database directly (only metadata)
- Authentication bypass

### API Endpoints

#### Authentication
- `POST /api/v1/auth/register` - User registration
- `POST /api/v1/auth/login` - User login
- `POST /api/v1/auth/logout` - User logout
- `POST /api/v1/auth/refresh` - Refresh JWT token
- `GET /api/v1/auth/me` - Get current user
- `POST /api/v1/auth/validate` - Validate token

#### Features
- JWT token generation and validation
- User authentication
- Password management
- Token refresh
- User profile management

### Database Access

**Internal Only:**
- `keycloak_db` - Internal Keycloak database
- `platform_db.users` - Read-only user metadata

**No Direct Access:**
- No direct database access to user passwords
- Keycloak manages authentication
- Only metadata read from platform_db

### Constraints

- Only gateway to Keycloak
- No direct database access
- No authentication bypass allowed
- JWT validation on all routes
- Rate limiting on auth endpoints

### Deployment

- Standalone Node.js service
- Docker containerization
- Health check endpoints
- Graceful shutdown
- Performance monitoring
- Security audit logging

---

## 🗄️ DATABASE OWNERSHIP

### Service Database Ownership

| Service | Database Access | Ownership |
|---------|----------------|-----------|
| driver-service | platform_db.inventory (read), platform_db.gis (read-only), analytics_db (write) | Read operations |
| admin-service | platform_db.inventory (read/write), platform_db.users (read/write), platform_db.gis (read-only), analytics_db (write) | Read/Write operations |
| auth-service | keycloak_db (internal), platform_db.users (read-only) | Authentication metadata |

### Database Rules

**system of record:** platform_db
**append-only:** analytics_db
**read-only:** gis schema
**auth-service ownership:** users schema (metadata)

---

## 🔗 API CONTRACTS

### Versioning

**Rule:** All endpoints MUST follow `/api/v1/*` pattern

**No unversioned routes allowed**

### Service Communication

**Rule:** Services communicate only through defined API endpoints

**No direct database access between services**

**No shared memory or processes**

---

## 🛡️ SECURITY

### Service Security

**Authentication:**
- MVP-1: No authentication (public API)
- MVP-2+: JWT authentication required
- MVP-3+: Keycloak integration

**Authorization:**
- Role-based access control (RBAC)
- Permission checks on protected endpoints
- Authorization middleware required
- No authorization bypass allowed

**Database Security:**
- Connection pooling
- Connection limits
- Timeout handling
- Encryption in transit (TLS)
- No hardcoded credentials

### API Security

**Rate Limiting:**
- API gateway rate limiting
- Service-level rate limiting
- Protection against abuse

**Input Validation:**
- All inputs validated before processing
- SQL injection prevention
- XSS prevention
- CSRF protection

---

## ⚡ PERFORMANCE

### Service Performance

**Optimization Strategies:**
- Connection pooling
- Efficient queries
- Caching where appropriate
- Async processing for heavy operations

**Monitoring:**
- Health check endpoints
- Performance metrics
- Error tracking
- Resource utilization

### Caching

**Redis Integration:**
- Frequently accessed data caching
- Cache invalidation strategies
- Cache warming for performance

---

## 🧪 TESTING

### Testing Requirements

**Service Testing:**
- Unit tests for services
- Integration tests for API
- E2E tests for critical flows
- Database tests

**Coverage:**
- Core functionality ≥ 80%
- API endpoints ≥ 90%
- Database operations ≥ 85%
- Business logic ≥ 80%

---

## 🔄 DOCUMENTATION IS SYSTEM

**Services architecture rules are documented here.**
**Code must implement documented architecture.**
**Documentation must be updated with changes.**

**Documentation is the system. Code is just its execution.**
