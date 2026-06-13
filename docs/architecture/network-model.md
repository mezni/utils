# Network Model

## Version: 1.0
## Status: Active
## Core Philosophy: Documentation is the system. Code is just its execution.

---

## 🌐 OVERVIEW

BorneMap uses a microservices architecture with a strict layer separation and API-first design pattern. All services communicate through defined endpoints following versioned API contracts.

**Delivery Model:** LLM-driven MVP execution system
**Execution Order:** SpecKit → OpenCode → Implementation → Validation → Bug Loop → MVP Freeze

---

## 📋 SYSTEM ARCHITECTURE

```
[Actors]
  👤 Public Driver
  👤 Registered Driver
  👤 Partner
  👤 Admin

[Frontend Layer]
  📱 mobile-driver (Expo)
  🌐 web-driver (React + Leaflet)
  🖥️ dashboard (React + shadcn/ui)
  📦 Shared Packages
    • @bm/types
    • @bm/api-client
    • @bm/utils
    • @bm/design-tokens

[Edge Layer]
  🌐 Traefik (API Gateway)
    • TLS termination
    • API routing
    • Load balancing

[Service Layer]
  ⚙️ driver-service (Rust) :3000  ← MVP-1 Only
  ⚙️ admin-service (Node.js) :3001 ← MVP-2+
  🔐 auth-service (Node.js) :3002 ← MVP-3+

[Data Layer]
  🗄️ platform_db (PostgreSQL + PostGIS)
    • inventory schema (station → charger)
    • gis schema (read-only)
    • users schema
  📊 analytics_db (append-only events)
  🔑 keycloak_db (internal only)

[Identity Layer]
  🔐 Keycloak

[External Layer]
  🌍 OpenStreetMap
  🗺️ Map Tiles Provider
```

---

## 🎯 ACTORS

### Public Driver
- No authentication required
- Accesses basic station discovery
- Can view nearby stations
- Limited features

### Registered Driver
- Authentication required
- Full station discovery access
- Account management
- Enhanced features

### Partner
- Business integration
- Station management capabilities
- Analytics access
- Partner-specific features

### Admin
- System administration
- Complete station management
- User management
- Analytics and reporting

---

## 🌐 EDGE LAYER - Traefik

**Role:** API Gateway and Load Balancer

**Functionality:**
- TLS termination
- API routing and versioning
- Load balancing
- Rate limiting
- Health checks
- Monitoring

**Routing Rules:**
- `/api/v1/*` → driver-service (Port 3000)
- `/api/v1/admin/*` → admin-service (Port 3001)
- `/api/v1/auth/*` → auth-service (Port 3002)

---

## 📱 FRONTEND LAYER

### Applications

#### mobile-driver (Expo SDK 54)
- **Purpose:** Driver mobile application
- **Tech Stack:** React Native, Expo SDK 54
- **Features:**
  - Station discovery
  - Nearby search
  - Station details
  - Basic analytics
- **Constraints:** Mobile-first, touch interactions

#### web-driver (React + Leaflet)
- **Purpose:** Web driver application
- **Tech Stack:** React, Leaflet, @bm/api-client
- **Features:**
  - Station discovery
  - Nearby search
  - Station details
  - Map interactions
- **Constraints:** Responsive, web-optimized

#### dashboard (React + shadcn/ui)
- **Purpose:** Admin and partner dashboard
- **Tech Stack:** React, shadcn/ui, @bm/api-client
- **Features:**
  - Station management
  - User management
  - Analytics
  - Operational tools
- **Constraints:** Admin-only access

### Shared Packages

#### @bm/types
- **Purpose:** Type definitions
- **Contents:** TypeScript interfaces and types
- **Usage:** All frontend and backend code

#### @bm/api-client
- **Purpose:** API communication layer
- **Contents:** Request/Response handlers
- **Usage:** All frontend apps
- **Constraints:** No direct API calls allowed

#### @bm/utils
- **Purpose:** Utility functions
- **Contents:** Common utilities
- **Usage:** All frontend and backend code

#### @bm/design-tokens
- **Purpose:** Design system
- **Contents:** Colors, spacing, typography
- **Usage:** All frontend UI components

---

## ⚙️ SERVICE LAYER

### driver-service :3000

**Scope:** MVP-1 only

**Responsibilities:**
- Station CRUD operations
- Nearby station search
- GIS data integration
- Basic analytics events
- Station status management

**Allowed APIs:**
- GET /api/v1/stations
- GET /api/v1/stations/nearby
- GET /api/v1/stations/{id}

**Database Access:**
- platform_db.inventory
- platform_db.gis (read-only)
- analytics_db (append-only)

---

### admin-service :3001

**Scope:** MVP-2+

**Responsibilities:**
- Station management
- User management
- Partner management
- Operational workflows
- Admin analytics

**Allowed APIs:**
- All /api/v1/admin/* endpoints
- Station CRUD operations
- User management
- Partner operations

**Database Access:**
- platform_db.inventory (read/write)
- platform_db.users (read/write)
- platform_db.analytics (append-only)

---

### auth-service :3002

**Scope:** MVP-3+

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

**Database Access:**
- keycloak_db (internal only)
- platform_db.users (read-only)

**Constraints:**
- Only gateway to Keycloak
- No direct database access
- No auth bypass allowed

---

## 🗄️ DATA LAYER

### platform_db (PostgreSQL + PostGIS)

**Purpose:** System of record

**Schemas:**

#### inventory schema
- **Owner:** admin-service
- **Contents:**
  - Stations
  - Chargers
  - Station relationships
  - Operational data
- **Access:** Read/write (admin-service)

#### gis schema
- **Owner:** Read-only
- **Contents:**
  - Station locations
  - Map data
  - Geographic features
- **Access:** Read-only (all services)
- **Constraints:** No modifications allowed

#### users schema
- **Owner:** auth-service (metadata)
- **Contents:**
  - User profiles
  - Authentication data
  - User preferences
- **Access:** Read-only (auth-service)
- **Constraints:** No direct access from other services

---

### analytics_db

**Purpose:** Append-only analytics storage

**Contents:**
- Event tracking data
- User behavior
- Station usage
- System performance

**Access:** Write-only (all services)
**Constraints:** Append-only, no deletions
**Architectural Rule:** Never delete from this database

---

### keycloak_db

**Purpose:** Internal authentication database

**Access:** Internal only
**Constraints:**
- No external access
- No direct database access
- Only auth-service can interact

---

## 🔐 IDENTITY LAYER - Keycloak

**Purpose:** Central authentication and authorization

**Flow:**
1. Frontend authenticates with auth-service
2. auth-service proxies to Keycloak
3. Keycloak validates credentials
4. Keycloak returns JWT token
5. Frontend uses token for subsequent requests
6. Services validate token for authorization

**Constraints:**
- Only auth-service communicates with Keycloak
- No frontend or backend bypass allowed
- JWT is the only trusted identity mechanism

---

## 🌍 EXTERNAL LAYER

### OpenStreetMap
- **Purpose:** Free map data source
- **Access:** Read-only
- **Usage:** GIS data reference
- **Constraints:** No direct database modifications

### Map Tiles Provider
- **Purpose:** Map rendering
- **Access:** Read-only
- **Usage:** Mobile and web map rendering
- **Constraints:** Mobile app battery optimization required

---

## 🔄 COMMUNICATION FLOWS

### Station Discovery Flow

```
Driver → Traefik → driver-service → platform_db.inventory
                   ↓
                 analytics_db
                   ↓
                 GIS data (read-only)
```

### Authentication Flow

```
Driver → auth-service → Keycloak → keycloak_db
           ↓
         JWT token returned
           ↓
         Services validate JWT
```

### Analytics Flow

```
User Action → Frontend → Services → analytics_db
                      (append-only)
```

---

## 📊 NETWORK RULES

### API Versioning
- All endpoints MUST follow `/api/v1/*` pattern
- No unversioned routes allowed
- Version changes require migration plan

### Service Communication
- Services communicate only through API endpoints
- No direct database access between services
- No shared memory or processes
- Synchronous API calls preferred

### Security Rules
- All traffic encrypted (TLS)
- JWT tokens must be validated
- Authorization checks before all operations
- Rate limiting on API gateway

---

## ⚡ PERFORMANCE RULES

### Frontend
- All requests through @bm/api-client
- No fetch() or axios usage
- Map rendering through MapContainer abstraction
- Loading and error states required

### Backend
- Database queries optimized
- Indexes properly maintained
- Connection pooling
- Caching where appropriate

### Network
- Load balancing across services
- CDN for static assets
- Connection reuse
- Timeout handling

---

## 🔄 DOCUMENTATION IS SYSTEM

**Architecture rules are documented here.**
**Code must implement documented architecture.**
**Documentation must be updated with changes.**

**Documentation is the system. Code is just its execution.**
