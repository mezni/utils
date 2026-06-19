# Architecture Documentation
## BorneMap System Design & Topology

**Version:** 1.0  
**Last Updated:** June 2026  
**Status:** Pre-Sprint Design

---

## 1. System Context Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                      👤 ACTORS                              │
├─────────────────────────────────────────────────────────────┤
│ Public Driver │ Registered Driver │ Partner/Operator │ Admin │
└────────────────────────────────────────────────────────────┘
                              │
                              ↓
┌─────────────────────────────────────────────────────────────┐
│          🌐 EDGE & IDENTITY SECURITY                       │
├─────────────────────────────────────────────────────────────┤
│  Traefik Gateway (:80/:443)                                │
│  ├─ TLS Termination                                         │
│  ├─ JWT Validation (JWKS Cache)                            │
│  └─ Route Dispatch                                          │
│                                                             │
│  Keycloak (Single Realm: bornemap)                         │
│  ├─ mobile-driver-app client                              │
│  ├─ web-driver-app client                                 │
│  └─ admin-dashboard client                                │
└─────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
        ↓                     ↓                     ↓
┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
│  /api/v1/auth/*  │ │ /api/v1/driver/* │ │ /api/v1/admin/*  │
└──────────────────┘ └──────────────────┘ └──────────────────┘
        │                     │                     │
        ↓                     ↓                     ↓
┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
│ Auth Service     │ │ Driver Service   │ │ Admin Service    │
│ :3000            │ │ :3001            │ │ :3002            │
│                  │ │                  │ │                  │
│ • Keycloak Proxy │ │ • Station Query  │ │ • Partner CRUD   │
│ • User Sync      │ │ • Geospatial API │ │ • Station Mgmt   │
│ • Token Mgmt     │ │ • Cache Layer    │ │ • Analytics      │
└──────────────────┘ └──────────────────┘ └──────────────────┘
        │                     │                     │
        │ Exclusive Write     │ Read Views          │ Read/Write
        │ users schema        │ + Redis Cache       │ inventory
        ↓                     ↓                     ↓
┌────────────────────────────────────────────────────────────┐
│           🗄️ RELATIONAL DATA ISOLATION TIER               │
├────────────────────────────────────────────────────────────┤
│                                                            │
│  platform_db (PostgreSQL 16 + PostGIS)                   │
│  ├── gis schema (Raw Import)                             │
│  │   └── osm_charging_stations_temp                      │
│  ├── inventory schema (Operational)                       │
│  │   ├── partners (OPR_*)                                │
│  │   ├── stations (STA_*)                                │
│  │   ├── chargers (CHG_*)                                │
│  │   └── materialized views                              │
│  │       ├── mv_stations_geo                             │
│  │       ├── mv_stations_summary                         │
│  │       └── mv_stations_reviews                         │
│  └── users schema (Auth-owned)                           │
│      └── user_profiles (USR_*)                           │
│                                                            │
│  keycloak_db (Isolated)                                  │
│  └─ Identity metadata only                               │
│                                                            │
│  analytics_db (Write-only)                               │
│  └─ Event logging                                        │
│                                                            │
└────────────────────────────────────────────────────────────┘
        │                     ↓
        │            ┌─────────────────┐
        │            │ Redis Cache     │
        │            │ Spatial Tiles   │
        │            └─────────────────┘
        │
        └─→ (Background) Ingestion ← OpenStreetMap
```

---

## 2. Service Topology (RIGID - 3 SERVICES ONLY)

### 2.1 Auth Service (:3000)

**Responsibility:**  
Keycloak gateway, user synchronization, token lifecycle management

**Ownership:**
- Exclusive write access to `users` schema
- Single point of contact with Keycloak
- User profile creation/updates

**API Routes:**
```
POST   /api/v1/auth/register          Create user account
POST   /api/v1/auth/login             Token exchange (via Keycloak)
POST   /api/v1/auth/refresh           Token refresh
POST   /api/v1/auth/logout            Token revocation
GET    /api/v1/auth/me                Current user profile
PUT    /api/v1/auth/profile           Update profile
```

**Database Access:**
```
Write:  users.user_profiles (exclusive)
Read:   (user profiles only)
```

**No Other Service May:**
- Call Keycloak directly
- Modify users schema
- Access user credentials
- Manage tokens (except validation via JWT)

---

### 2.2 Driver Service (:3001)

**Responsibility:**  
Station discovery, geospatial queries, cache layer, user experience

**Ownership:**
- Read-only access to `inventory` schema (views)
- Redis spatial cache read/write
- Driver-facing queries

**API Routes:**
```
GET    /api/v1/driver/stations        List stations (with pagination)
GET    /api/v1/driver/stations/:id    Station details
GET    /api/v1/driver/search          Search by location (radius)
GET    /api/v1/driver/chargers/:id    Charger details
GET    /api/v1/driver/favorites       List saved stations (auth-required)
POST   /api/v1/driver/favorites       Save station (auth-required)
DELETE /api/v1/driver/favorites/:id   Remove favorite
GET    /api/v1/driver/reviews         Station reviews
POST   /api/v1/driver/reviews         Submit review (auth-required)
```

**Database Access:**
```
Read:   inventory.stations, inventory.chargers, materialized views
Cache:  Redis (read/write spatial snapshots)
```

**Restrictions:**
- Cannot modify inventory directly
- Cannot access users schema
- Cannot write analytics events
- Cannot call Keycloak

---

### 2.3 Admin Service (:3002)

**Responsibility:**  
Partner management, station/charger CRUD, analytics pipeline, audit logging

**Ownership:**
- Exclusive write access to `inventory` schema
- Event logging to `analytics_db`
- Redis cache invalidation

**API Routes:**
```
POST   /api/v1/admin/partners         Create partner
GET    /api/v1/admin/partners         List partners (paginated)
GET    /api/v1/admin/partners/:id     Partner details
PUT    /api/v1/admin/partners/:id     Update partner
DELETE /api/v1/admin/partners/:id     Delete partner

POST   /api/v1/admin/stations         Create station
GET    /api/v1/admin/stations         List stations
PUT    /api/v1/admin/stations/:id     Update station
DELETE /api/v1/admin/stations/:id     Delete station

POST   /api/v1/admin/chargers         Create charger
PUT    /api/v1/admin/chargers/:id     Update charger
DELETE /api/v1/admin/chargers/:id     Delete charger

GET    /api/v1/admin/analytics        Analytical queries
GET    /api/v1/admin/audit            Audit logs
```

**Database Access:**
```
Write:  inventory.partners, inventory.stations, inventory.chargers
Write:  analytics_db (event logs)
Cache:  Redis (invalidation only)
```

**Responsibilities:**
- Maintain data consistency in inventory
- Log all CRUD operations to analytics
- Cache coherency (bust on changes)
- Audit trail for compliance

---

## 3. Frontend Applications (3 APPS - FIXED)

### 3.1 mobile-driver (Expo SDK 54)

**Platform:** iOS + Android (via Expo Go / Compiled)  
**Framework:** React Native + Expo  
**Store:** `source/apps/mobile-driver/`

**Features:**
- Station discovery (map + list)
- Favorites management
- Offline caching (AsyncStorage)
- Reviews & ratings
- User profile

**API Access:**
- Routes through Traefik gateway
- Client: `mobile-driver-app` (Keycloak)
- Token storage: Secure storage (not localStorage)

---

### 3.2 web-driver (React)

**Platform:** Web Browser  
**Framework:** React 18 + Leaflet + TypeScript  
**Store:** `source/apps/web-driver/`

**Features:**
- Interactive station map (Leaflet)
- Station search & filtering
- Favorites (with local persistence)
- Reviews & ratings
- User authentication flow

**API Access:**
- Routes through Traefik gateway
- Client: `web-driver-app` (Keycloak)
- Token storage: Memory only (never localStorage)

---

### 3.3 dashboard (React)

**Platform:** Web Browser  
**Framework:** React 18 + shadcn/ui + Tailwind  
**Store:** `source/apps/dashboard/`

**Features:**
- Partner: Station/charger management
- Admin: Analytics dashboards
- Admin: Audit logs
- Admin: User management
- Role-based UI rendering

**API Access:**
- Routes through Traefik gateway
- Clients: `admin-dashboard` (Keycloak)
- Token storage: Memory only

---

## 4. Data Schema Architecture

### 4.1 platform_db Structure

```sql
-- GIS Schema (Raw Import Tier)
CREATE SCHEMA gis;

CREATE TABLE gis.osm_charging_stations_temp (
    id BIGINT PRIMARY KEY,
    name TEXT,
    location GEOMETRY(POINT, 4326),
    metadata JSONB,
    imported_at TIMESTAMP
);

-- Inventory Schema (Operational & Projection)
CREATE SCHEMA inventory;

CREATE TABLE inventory.partners (
    id TEXT PRIMARY KEY,  -- OPR-{nanoid(12)}
    name TEXT NOT NULL,
    slug TEXT UNIQUE,
    contact_email TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE inventory.stations (
    id TEXT PRIMARY KEY,  -- STA-{nanoid(12)}
    partner_id TEXT NOT NULL,
    name TEXT NOT NULL,
    location GEOMETRY(POINT, 4326),
    address TEXT,
    city TEXT,
    region TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    FOREIGN KEY (partner_id) REFERENCES inventory.partners(id)
);

CREATE TABLE inventory.chargers (
    id TEXT PRIMARY KEY,  -- CHG-{nanoid(12)}
    station_id TEXT NOT NULL,
    type TEXT,  -- DC, AC, etc.
    power_kw DECIMAL,
    availability_status TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    FOREIGN KEY (station_id) REFERENCES inventory.stations(id)
);

-- Materialized Views (Driver Service Read Tier)
CREATE MATERIALIZED VIEW inventory.mv_stations_geo AS
    SELECT s.id, s.name, s.location, p.name AS partner_name
    FROM inventory.stations s
    JOIN inventory.partners p ON s.partner_id = p.id;

CREATE MATERIALIZED VIEW inventory.mv_stations_summary AS
    SELECT 
        s.id,
        s.name,
        p.name AS partner,
        COUNT(c.id) AS charger_count
    FROM inventory.stations s
    LEFT JOIN inventory.partners p ON s.partner_id = p.id
    LEFT JOIN inventory.chargers c ON s.id = c.station_id
    GROUP BY s.id, s.name, p.name;

-- Users Schema (Auth Service Exclusive)
CREATE SCHEMA users;

CREATE TABLE users.user_profiles (
    id TEXT PRIMARY KEY,  -- USR-{nanoid(12)}
    keycloak_id UUID NOT NULL UNIQUE,
    username TEXT NOT NULL UNIQUE,
    email TEXT NOT NULL,
    role TEXT,  -- 'driver', 'partner', 'admin'
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
```

### 4.2 keycloak_db (Isolated)

- Managed entirely by Keycloak
- No application access
- Contains identity metadata only

### 4.3 analytics_db (Event Log)

```sql
CREATE TABLE events (
    id UUID PRIMARY KEY,
    event_type TEXT,  -- 'station_created', 'station_updated', etc.
    entity_id TEXT,
    entity_type TEXT,
    actor_id TEXT,    -- USR_*
    timestamp TIMESTAMP,
    details JSONB
);
```

---

## 5. Caching Strategy (Redis)

### 5.1 Cache Layer Ownership

**Driver Service Exclusive Responsibility:**
- Initialize spatial tile snapshots
- Refresh on scheduled intervals
- Query for fast responses

**Admin Service Cache Invalidation:**
- Bust cache on station/charger updates
- Atomic operations with database writes

### 5.2 Cache Keys

```
tiles:geo:{zoom}:{lat}:{lon}:{radius}  → GeoJSON blob
tiles:stations:{station_id}              → Station details
tiles:chargers:{station_id}              → Charger list
tiles:reviews:{station_id}               → Reviews snapshot
```

---

## 6. API Gateway Routing (Traefik)

### 6.1 Routing Rules

```yaml
# Traefik Configuration
services:
  traefik:
    ports:
      - "80:80"
      - "443:443"
    
    routes:
      auth:
        rule: "PathPrefix(`/api/v1/auth`)"
        service: auth-service
      
      driver:
        rule: "PathPrefix(`/api/v1/driver`)"
        service: driver-service
      
      admin:
        rule: "PathPrefix(`/api/v1/admin`)"
        service: admin-service
    
    middlewares:
      jwt-validation:
        - source: JWKS endpoint (Keycloak)
        - fail-closed: true
        - excluded routes: /api/v1/auth/register, /api/v1/auth/login
```

### 6.2 Service Discovery

Each service registers:
```
auth-service:3000    → localhost:3000
driver-service:3001  → localhost:3001
admin-service:3002   → localhost:3002
```

---

## 7. Identity & Authentication Flow

### 7.1 Login Flow (Clients)

```
Client (mobile/web)
    │
    ├─→ POST /api/v1/auth/login {username, password}
    │
    └─← Auth Service
        │
        ├─→ POST /auth/realms/bornemap/protocol/openid-connect/token
        │
        └─← Keycloak (JWT token)
            │
            └─→ Response JWT to client
```

### 7.2 Authenticated Requests

```
Client + JWT token
    │
    ├─→ GET /api/v1/driver/stations
    │   + Authorization: Bearer {JWT}
    │
    └─← Traefik (JWT validation via JWKS)
        │
        ├─ Valid JWT? → Forward to Driver Service
        └─ Invalid? → 401 Unauthorized
```

---

## 8. Data Flow Examples

### 8.1 Station Discovery (Driver Flow)

```
Driver (Web/Mobile)
    │
    ├─→ GET /api/v1/driver/search?lat=36.8&lon=10.1&radius=5km
    │
    └─← Driver Service
        │
        ├─ Check Redis cache
        │  │
        │  ├─ HIT → Return cached GeoJSON
        │  │
        │  └─ MISS → Query inventory.mv_stations_geo
        │      │
        │      ├─ PostGIS spatial query
        │      │
        │      ├─ Update Redis cache
        │      │
        │      └─ Return GeoJSON
        │
        └─→ Response (stations list)
```

### 8.2 Station Creation (Admin Flow)

```
Admin (Dashboard)
    │
    ├─→ POST /api/v1/admin/stations {name, location, ...}
    │
    └─← Admin Service (role check: admin required)
        │
        ├─ Validate input
        │
        ├─ Generate ID: STA-{nanoid(12)}
        │
        ├─ BEGIN TRANSACTION
        │  │
        │  ├─ INSERT inventory.stations
        │  │
        │  ├─ INSERT analytics_db.events (audit log)
        │  │
        │  ├─ PUBLISH Redis cache invalidation
        │  │
        │  └─ COMMIT
        │
        └─→ Response (station created)
```

---

## 9. Deployment Topology (Docker Compose)

```yaml
version: "3.9"

services:
  traefik:
    image: traefik:latest
    ports: [80, 443]
    volumes:
      - ./infrastructure/traefik:/etc/traefik

  keycloak:
    image: keycloak:latest
    environment:
      - KEYCLOAK_ADMIN=admin
      - KEYCLOAK_ADMIN_PASSWORD=${KC_PASSWORD}
    volumes:
      - ./infrastructure/keycloak/realm-export.json:/opt/keycloak/data/import

  auth-service:
    build: ./services/auth-service
    environment:
      - DATABASE_URL=postgres://...
      - KEYCLOAK_URL=http://keycloak:8080

  driver-service:
    build: ./services/driver-service
    environment:
      - DATABASE_URL=postgres://...
      - REDIS_URL=redis://redis:6379

  admin-service:
    build: ./services/admin-service
    environment:
      - DATABASE_URL=postgres://...
      - ANALYTICS_DB_URL=postgres://...
      - REDIS_URL=redis://redis:6379

  postgres:
    image: postgres:16-alpine
    environment:
      - POSTGRES_DB=platform_db
    volumes:
      - ./infrastructure/postgres/init:/docker-entrypoint-initdb.d
      - pg_data:/var/lib/postgresql/data

  redis:
    image: redis:7-alpine
    command: redis-server /etc/redis/redis.conf
    volumes:
      - ./infrastructure/redis/redis.conf:/etc/redis/redis.conf

  web-driver:
    build: ./apps/web-driver
    environment:
      - VITE_API_URL=http://localhost/api/v1

  dashboard:
    build: ./apps/dashboard
    environment:
      - VITE_API_URL=http://localhost/api/v1

volumes:
  pg_data:
```

---

## 10. Architectural Constraints & Rules

### 10.1 Service Isolation (STRICT)

| Service | Can Read | Can Write | Cannot Touch |
|---------|----------|-----------|--------------|
| Auth | users | users | inventory, analytics |
| Driver | inventory (views), Redis | Redis | users, analytics |
| Admin | inventory, analytics | inventory, analytics | users, keycloak_db |

### 10.2 Frontend Access (STRICT)

```
Forbidden:
❌ Direct fetch/axios to backend endpoints
❌ Hardcoded service URLs
❌ Bypassing Traefik gateway
❌ Direct Keycloak API calls

Required:
✅ Use api-client (generated from OpenAPI)
✅ Route through Traefik (/api/v1/*)
✅ JWT via Keycloak Auth Service
✅ Response validation (Zod)
```

### 10.3 Database Access (STRICT)

```
Allowed:
✅ SQLx query!() macro (compile-time)
✅ Transactional writes
✅ Prepared statements

Forbidden:
❌ Raw SQL concatenation
❌ String interpolation in queries
❌ Unwrap/expect outside tests
❌ Cross-schema writes
```

---

## 11. Scaling & High Availability Notes

### Current Phase
- Single instance per service (Docker Compose)
- Single Redis instance
- Single PostgreSQL instance

### Future (Not in validation phase)
- Load balancing via Traefik
- PostgreSQL replication (read replicas)
- Redis cluster for spatial cache
- Service horizontal scaling
- Circuit breakers for resilience

---

## 12. References

- See `auth-flow.md` for detailed authentication flows
- See `api-contracts.md` for OpenAPI specifications
- See `GUARDRAILS.md` for execution standards
- See `SYSTEM_STATE.md` for current status
