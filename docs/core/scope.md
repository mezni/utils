# BorneMap Platform Scope

This document defines what the BorneMap platform includes and what is explicitly deferred or out of scope.

---

## 1. Platform Purpose

BorneMap is an **EV station discovery and management system for Tunisia**.

It enables:
- **Public Drivers** to discover nearby EV charging stations without login
- **Registered Drivers** to save favorites, write reviews, and manage profiles
- **Partners** to manage their own stations and chargers through a dashboard
- **Admins** to manage the entire platform globally

---

## 2. Included Features

### Discovery & Browsing
- ✅ View all stations on a map (public, no login required)
- ✅ Search for stations by location, availability, connector type
- ✅ Filter stations by amenities, availability, ratings
- ✅ View station details (address, chargers, reviews, ratings)
- ✅ Nearby station search (GPS-based for drivers)

### Driver Features
- ✅ View public reviews and ratings
- ✅ Favorite stations (registered drivers only)
- ✅ Write, edit, delete reviews (registered drivers only)
- ✅ User profile management (registered drivers only)

### Partner Features
- ✅ Partner dashboard
- ✅ View and manage own stations
- ✅ View and manage own chargers
- ✅ Manually update charger availability
- ✅ View own analytics and reports

### Admin Features
- ✅ Admin dashboard
- ✅ Manage all users
- ✅ Manage all partners
- ✅ Manage all stations and chargers globally
- ✅ Moderate reviews and ratings
- ✅ Access global reporting and analytics

### Technical Features
- ✅ User authentication (email, Google, Facebook via Keycloak)
- ✅ Role-based access control (Public Driver, Registered Driver, Partner, Admin)
- ✅ GIS data enrichment (OpenStreetMap integration)
- ✅ Spatial indexing for nearby searches
- ✅ Clickstream analytics
- ✅ Arabic language support (Arabic, French, English)
- ✅ RTL layout support for Arabic
- ✅ Public API for discovery endpoints
- ✅ Partner-scoped APIs for partners
- ✅ Admin APIs for platform management

---

## 3. Explicitly Out of Scope (Deferred)

### OCPP (Open Charge Point Protocol)
- ❌ OCPP charger communication
- ❌ Real-time charger status via OCPP
- ❌ Charger commands (unlock, reset)
- ❌ OCPP session management

**Status:** Deferred to a future phase. Partner availability updates remain **manual** until OCPP is introduced.

**Rationale:** OCPP adds significant complexity in charger firmware management, error handling, and operational burden. Manual updates via the partner dashboard are sufficient for Phase 1.

### Payment & Billing
- ❌ In-app payments
- ❌ Payment processing integration
- ❌ Billing and invoicing
- ❌ Subscription management
- ❌ Revenue reporting

**Status:** Deferred. Partners may implement their own payment systems independently.

**Rationale:** Payment introduces PCI compliance, third-party integrations, and regulatory complexity beyond the platform's current scope.

### Routing & Navigation
- ❌ Turn-by-turn navigation
- ❌ Route optimization
- ❌ Multi-stop trip planning
- ❌ ETA calculations

**Status:** Deferred. Drivers use external maps (Google Maps, Apple Maps) for navigation.

**Rationale:** Navigation requires real-time traffic data, complex algorithms, and is better served by existing map services.

### Real-Time Availability (OCPP-Driven)
- ❌ Real-time charger availability from OCPP
- ❌ Live occupancy updates
- ❌ Queue management
- ❌ Reservation system

**Status:** Deferred until OCPP is implemented.

**Current behavior:** Manual updates by partners. Availability shown as last-known state.

### Push Notifications
- ❌ Push notifications to drivers
- ❌ Notification preferences
- ❌ Notification history

**Status:** Deferred to a future phase.

**Rationale:** Adds complexity in device registration, carrier dependencies, and privacy compliance. Email notifications via Keycloak are sufficient initially.

---

## 4. Infrastructure & Deployment

### Included
- ✅ PostgreSQL 16 + PostGIS for data
- ✅ Keycloak for authentication
- ✅ Docker Compose for orchestration
- ✅ Traefik for routing and TLS
- ✅ Rust services (Driver, Admin, Clickstream)
- ✅ React web applications
- ✅ React Native mobile app
- ✅ Monorepo structure (Cargo + npm workspaces)

### Out of Scope
- ❌ Kubernetes
- ❌ Cloud-managed databases
- ❌ Message queues (RabbitMQ, Kafka)
- ❌ Image registry
- ❌ Distributed caching (Redis)
- ❌ Serverless functions

**Rationale:** Bare metal + Docker Compose keeps operations simple (one-person operable). Complexity introduced only when scale justifies it.

---

## 5. User Types & Capabilities Matrix

| Feature | Public Driver | Registered Driver | Partner | Admin |
|---------|---|---|---|---|
| View stations | ✅ | ✅ | ✅ | ✅ |
| Search/filter | ✅ | ✅ | ✅ | ✅ |
| View details | ✅ | ✅ | ✅ | ✅ |
| View reviews | ✅ | ✅ | ✅ | ✅ |
| Write reviews | ❌ | ✅ | ✅ | ✅ |
| Favorite stations | ❌ | ✅ | ❌ | ❌ |
| Profile management | ❌ | ✅ | ✅ | ✅ |
| Partner dashboard | ❌ | ❌ | ✅ | ❌ |
| Manage own stations | ❌ | ❌ | ✅ | ❌ |
| Manage own chargers | ❌ | ❌ | ✅ | ❌ |
| Update availability | ❌ | ❌ | ✅ | ✅ |
| Admin dashboard | ❌ | ❌ | ❌ | ✅ |
| Manage all users | ❌ | ❌ | ❌ | ✅ |
| Manage all stations | ❌ | ❌ | ❌ | ✅ |
| Moderate reviews | ❌ | ❌ | ❌ | ✅ |
| Global reporting | ❌ | ❌ | ❌ | ✅ |

---

## 6. Language & Localization

### Supported Languages
- ✅ **French** (Primary)
- ✅ **Arabic** (with RTL layout)
- ✅ **English** (English)

### Not Supported
- Berber (Tamazight) — out of scope
- Other languages — out of scope

### RTL Requirement
All screens must work correctly in Arabic RTL layout. This is **not deferred**. Any RTL failure is a **Class A bug**.

---

## 7. Geographic Scope

### In Scope
- ✅ **Tunisia** — entire country
- ✅ All regions with available station data

### Out of Scope
- ❌ Other countries
- ❌ Regional focus on specific governorates (initially)

The platform architecture supports expansion to other countries in the future.

---

## 8. Data Scope

### Included Data Sources
- ✅ OpenStreetMap (OSM) for geography and boundaries
- ✅ Partner-submitted station and charger data
- ✅ Manual availability updates by partners
- ✅ User-generated reviews and ratings
- ✅ Clickstream analytics

### Not Included
- ❌ Real-time traffic data
- ❌ Real-time charger status (until OCPP)
- ❌ User location history (privacy)
- ❌ Third-party aggregator feeds

---

## 9. Regulatory & Compliance

### In Scope
- ✅ GDPR compliance (user data)
- ✅ Arabic language support (regulatory requirement)
- ✅ Secure authentication

### Out of Scope (Deferred)
- ❌ PCI DSS (payment processing not included)
- ❌ Accessibility standards beyond WCAG 2.1 AA
- ❌ Industry-specific certifications (ISO, SOC 2)

---

## 10. Timeline

This scope defines **Phase 1** of the BorneMap platform. Future phases may include:

- **Phase 2:** OCPP integration, real-time availability
- **Phase 3:** Payments and billing
- **Phase 4:** Routing and navigation
- **Phase 5:** Push notifications, advanced analytics

Each phase will have its own scope document and approved ADRs.

---

## 11. Change Control

To change this scope:

1. Document the requested change
2. Evaluate impact on architecture, services, and data models
3. Create an ADR if it introduces new services or crosses architectural boundaries
4. Update this document
5. Get stakeholder approval

Any **scope change requires an ADR** if it:
- Introduces a new service
- Changes the authentication or authorization model
- Affects database schema design
- Changes the user role model

---

**Document Version:** 1.0  
**Status:** Active  
**Last Updated:** 2026-06-05  
**Next Review:** End of Phase 1
