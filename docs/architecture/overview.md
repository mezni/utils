# Architecture Overview

BorneMap is a pragmatic, service-oriented platform built on PostgreSQL, Keycloak, and Rust backend services. This document provides a high-level view of the system design.

---

## System Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         Clients                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│   Driver Web App     Driver Mobile App      Dashboard App        │
│   (React + Vite)    (React Native+Expo)     (React + Vite)      │
│                                                                   │
└──────────────┬──────────────┬────────────────┬──────────────────┘
               │              │                │
               └──────────────┼────────────────┘
                              │
                         ┌────▼────┐
                         │ Traefik  │ (Port 80/443, TLS, Rate Limiting)
                         └────┬────┘
          ┌──────────────────┼──────────────────┐
          │                  │                  │
      ┌───▼──────┐    ┌──────▼──────┐   ┌──────▼──────┐
      │ Keycloak │    │   Services   │   │   Analyzer  │
      │          │    │ (Rust/Actix) │   │  (Future)   │
      └───┬──────┘    │              │   └─────────────┘
          │           │ - Driver     │
          │           │ - Admin      │
          │           │ - Clickstream│
          │           └──────┬───────┘
          │                  │
          └──────────┬───────┴────────────────┐
                     │                        │
                  ┌──▼────────────────────────▼──┐
                  │   PostgreSQL 16 + PostGIS   │
                  │                              │
                  │  ┌─────────────────────┐    │
                  │  │ inventory (Admin)   │    │
                  │  ├─────────────────────┤    │
                  │  │ users (Driver)      │    │
                  │  ├─────────────────────┤    │
                  │  │ gis (Trigger)       │    │
                  │  ├─────────────────────┤    │
                  │  │ analytics (Events)  │    │
                  │  └─────────────────────┘    │
                  └─────────────────────────────┘
```

---

## Core Principles

1. **Pragmatic Architecture** — Minimum services, clear boundaries
2. **Single Source of Truth** — inventory.station owns station data
3. **Simple Operations** — One person can operate it
4. **Domain Separation by Schema** — inventory, users, gis, analytics
5. **Build for Current Scale** — No premature optimization
6. **Public Access First** — No login required for browsing
7. **RTL & Arabic Built-In** — Not an afterthought
8. **Visual Consistency** — Tokens define all visual values

---

## Services Overview

### Authentication Layer
**Keycloak** — Identity and access management
- Email, Google, Facebook authentication
- JWT issuance and signing
- Role assignment (registered_driver, partner, admin)
- partner_id claim injection
- Session management and token refresh

### API Layer
**Driver Service** (Rust/Actix-web)
- Station discovery (public endpoint)
- Map markers and nearby search
- Station details, search, filtering
- Authenticated driver features (favorites, reviews, profiles)
- First-login user provisioning

**Admin Service** (Rust/Actix-web)
- Partner CRUD operations
- Station CRUD operations
- Charger CRUD operations
- Manual availability updates
- Review moderation
- Reporting and analytics queries

**Clickstream Service** (Rust/Actix-web)
- Event ingestion (public endpoint)
- Event validation against canonical taxonomy
- Direct write to analytics schema
- No authentication required

### Edge Router
**Traefik**
- Public port exposure (80, 443)
- TLS certificate management (Let's Encrypt)
- HTTP to HTTPS redirect
- Routing to internal services by domain and path
- Rate limiting

### Data Layer
**PostgreSQL 16 + PostGIS**
- Single database with four schemas
- Spatial indexing for nearby searches
- Trigger-based GIS synchronization
- All migrations managed by respective services

---

## Frontend Applications

### Driver Web App
- **Technology:** React + Vite
- **Purpose:** Public and authenticated driver experience
- **Layout:** Full-bleed map with floating UI elements
- **Tokens:** Plus Jakarta Sans, map-specific
- **Primary Users:** Public and registered drivers on desktop

### Driver Mobile App
- **Technology:** React Native + Expo
- **Purpose:** iOS and Android driver experience
- **Layout:** Full-bleed map with bottom sheet pattern
- **Tokens:** Plus Jakarta Sans via React Native
- **Primary Users:** Drivers on mobile devices

### Dashboard App
- **Technology:** React + Vite
- **Purpose:** Single app for partner and admin roles
- **Role Switching:** Determined by JWT on login
- **Layout:** Sidebar navigation
- **Tokens:** Inter, no map-specific
- **Primary Users:** Partners and admins

All three share:
- Design token foundation (packages/ui)
- API client packages
- Authentication client (Keycloak)
- Accessibility standards (WCAG 2.1 AA)
- Language support (French, Arabic, English)

---

## Data Architecture

### Four Schemas

| Schema | Owner | Writes | Reads |
|--------|-------|--------|-------|
| **inventory** | Admin Service | Admin Service | Admin, Driver, Trigger |
| **users** | Driver Service | Driver Service | Driver, Admin (reporting) |
| **gis** | Trigger Function | Trigger only | Driver (spatial) |
| **analytics** | Clickstream Service | Clickstream | Admin (reporting) |

### Critical Rules
- **inventory.station** is source of truth for stations
- **gis** is derived enrichment, never authoritative
- **No app code writes to gis** — trigger function only
- **Analytics isolated** to analytics schema
- Cross-schema access strictly limited

---

## Request Flow

### Public Station Discovery
```
Driver Web/Mobile (unauthenticated)
    ↓
Traefik (routing)
    ↓
Driver Service (public endpoint)
    ↓
PostgreSQL (inventory + gis schemas)
    ↓
JSON response (stations, markers, details)
```

### Authenticated Driver Action (Favorite)
```
Driver Web/Mobile (with JWT)
    ↓
Traefik (TLS, routing)
    ↓
Driver Service (auth middleware validates JWT)
    ↓
First-login check (users.user_account)
    ↓
PostgreSQL (users schema)
    ↓
JSON response (success)
```

### Partner Management
```
Dashboard (partner JWT)
    ↓
Traefik
    ↓
Admin Service (auth + partner scope middleware)
    ↓
Scope enforcement (partner_id from JWT)
    ↓
PostgreSQL (inventory schema)
    ↓
JSON response (partner-scoped data only)
```

### Analytics Event
```
Frontend (any user)
    ↓
Traefik
    ↓
Clickstream Service (public endpoint)
    ↓
Event validation (against taxonomy)
    ↓
PostgreSQL (analytics.raw_events)
    ↓
Success/error response
```

---

## Technology Stack

| Layer | Technology |
|-------|-----------|
| **Frontend** | React, React Native, Vite |
| **Backend** | Rust, Actix-web |
| **Database** | PostgreSQL 16 + PostGIS |
| **Auth** | Keycloak 24 |
| **Router** | Traefik v3 |
| **Orchestration** | Docker Compose |
| **Package Manager** | npm (frontend), Cargo (backend) |
| **Monorepo** | npm workspaces + Cargo workspaces |

---

## Deployment Topology

### Production
```
Internet
  ↓
Traefik (Port 80/443) ← Public entry point
  ↓
Docker Network (internal)
  ├─ PostgreSQL (port 5432, internal only)
  ├─ Keycloak (port 8080, routed by Traefik)
  ├─ Driver Service (port 8080, routed by Traefik)
  ├─ Admin Service (port 8080, routed by Traefik)
  └─ Clickstream Service (port 8080, routed by Traefik)
```

### Development
```
Same as production +
  ├─ pgAdmin (optional, for database inspection)
```

**Key Rule:** Only Traefik exposes public ports. All services use internal Docker networking.

---

## Runtime Guarantees

### Service Startup
1. Each Rust service runs `sqlx::migrate!` before accepting requests
2. Services declare `depends_on` with `condition: service_healthy`
3. Database must be healthy before services start
4. Failed migrations cause immediate startup failure with clear error

### Health Checks
- **PostgreSQL:** `pg_isready` every 10 seconds
- **Rust Services:** HTTP `/health` endpoint every 30 seconds
- **Keycloak:** Spring Boot health check every 30 seconds
- All services must pass health checks before routing traffic

### Graceful Shutdown
- Services have 30-second grace period to finish in-flight requests
- Connections closed cleanly
- Database transactions rolled back on timeout

---

## Security Posture

### Authentication
- Keycloak owns all auth (no service implements login)
- JWT validated against JWKS (cached, background refresh)
- Bearer tokens in HTTP Authorization header
- Tokens stored securely (memory for web, expo-secure-store for mobile)

### Authorization
- Role-based access control via JWT claims
- Partner scope enforcement in middleware
- Roles checked before any handler runs
- Partner users cannot access other partner data

### Encryption
- TLS everywhere (Traefik → Services, Services → Database)
- Secrets on host only (never in images or committed files)
- PostgreSQL user/password separate from application code

### Data Isolation
- Four schemas with strict cross-schema rules
- Partner data scoped by JWT claim
- Public data accessible without auth
- Analytics isolated from business data

---

## Resilience & Recovery

### Database Backups
- Automated daily backups to host
- Point-in-time recovery supported
- Backup tested weekly

### Failure Scenarios
- **Service Down:** Health checks detect, Traefik stops routing, manual restart via runbook
- **Database Down:** All services fail gracefully, health checks reflect status
- **Disk Full:** Monitoring alerts, manual disk cleanup required
- **Certificate Expiration:** Let's Encrypt renewal automatic via Traefik

### Operational Procedures
Every operational task has a documented runbook:
- Deployment
- Database migration
- User recovery
- Service restart
- Emergency rollback

---

## Observability

### Logging
- All services log to stdout (Docker captures to host)
- Structured JSON logs for parsing
- Log levels: DEBUG, INFO, WARN, ERROR

### Health Checks
- Services expose `/health` endpoint
- Health check response indicates database connectivity
- Traefik routes only to healthy services

### Metrics
- Basic metrics: request count, response time, error rate
- Captured via Clickstream (user events)
- Admin reporting aggregates analytics data

---

## Scalability Approach

**Current Philosophy:** Build for current scale. Introduce complexity only when justified.

- **Caching:** Not needed (fast spatial queries via GIST indexes)
- **Sharding:** Not needed (PostgreSQL handles station count)
- **Read Replicas:** Not needed (low read/write ratio)
- **Message Queues:** Not needed (direct database writes suffice)

**Future:** If scale increases, consider read replicas or caching (requires ADR).

---

## Decision Log

All non-trivial architecture decisions recorded in `docs/adr/`:
- ADR-001 — PostgreSQL + PostGIS as single database
- ADR-002 — Schema separation over database separation
- ADR-003 — Prefixed NanoIDs over UUIDs
- ADR-004 — Direct analytics insert over RabbitMQ
- ADR-005 — Rust for backend services
- ADR-006 — Bare metal + Docker Compose over Kubernetes
- ADR-007 — Keycloak for authentication
- ADR-008 — PostgreSQL trigger for GIS synchronization
- ADR-009 — Monorepo with Cargo and npm workspaces
- ADR-010 — Traefik as edge router
- ADR-011 — React + Vite for web applications
- ADR-012 — React Native + Expo for mobile app
- ADR-013 — Single Dashboard App over separate Partner and Admin apps

---

**Document Version:** 1.0  
**Status:** Active  
**Last Updated:** 2026-06-05
