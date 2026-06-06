# BorneMap Glossary

Essential terminology used throughout the BorneMap platform, architecture, and documentation.

---

## User Roles & Access

### Public Driver
An unauthenticated user who can browse stations without login. Primary use case: discovering EV charging stations while on the road.

### Registered Driver
An authenticated user who has completed first-login provisioning. Can manage favorites, write reviews, and manage profile. Keycloak role: `registered_driver`.

### Partner
An authenticated business or organization that manages EV charging stations. Partners can only manage their own stations and chargers. They belong to exactly one partner entity. Keycloak role: `partner`.

### Admin
An authenticated platform administrator with full access to all entities, users, partners, and global reporting. Keycloak role: `admin`.

### First-Login Provisioning
The process of creating a `users.user_account` record when an authenticated user calls a Driver Service endpoint for the first time. Creates a USR-... identifier and tracks last_login_at.

---

## Core Entities

### Station
A physical location that provides EV charging. Identified by STN-... NanoID. Owned by `inventory.station` table. Source of truth for all station data including location, chargers, and partner affiliation.

**Attributes:** station_id, partner_id, name, latitude, longitude, address, city, governorate, amenities, created_at, updated_at

### Charger
An individual EV charging port at a station. Identified by CHG-... NanoID. Owned by `inventory.charger` table.

**Attributes:** charger_id, station_id, connector_type, power_output, availability_status, last_updated_at

**Connector Types:** Type 1, Type 2, CCS, CHAdeMO, Tesla Supercharger

**Availability Status:** Available, In Use, Maintenance, Offline

### Favorite
A saved station bookmark by a registered driver. Owned by `users.favorite_station` table. Allows drivers to quickly access frequently-visited stations.

### Review
User-generated feedback about a station. Identified by REV-... NanoID. Owned by `users.station_review` table. Includes rating (1-5 stars) and text comment.

### Partner
An organization or business that owns and operates EV charging stations. Identified by PRT-... NanoID. Owned by `inventory.partner` table.

---

## Authentication & Authorization

### JWT (JSON Web Token)
A signed token issued by Keycloak that contains user identity, roles, and claims. Used to authenticate API requests. Must be stored securely (memory only for web apps, expo-secure-store for mobile).

### Bearer Token
A JWT sent in the HTTP Authorization header (`Authorization: Bearer <token>`) to authenticate a request.

### JWKS (JSON Web Key Set)
Keycloak's public key set used to validate JWT signatures. Cached in application state with background refresh. Never fetched per-request.

### Keycloak
Open-source identity and access management server. Owns all authentication including email/OAuth2 login, JWT issuance, role assignment, and token refresh.

### Realm (Keycloak)
A logical partition in Keycloak that defines a set of users, roles, and credentials. BorneMap uses a single realm with four roles: registered_driver, partner, admin, and implicit public_driver.

### Mapper (Keycloak)
A function that adds custom claims to JWT tokens. BorneMap uses a mapper to inject `partner_id` claim for partner users.

### Role
A named permission set assigned to a user. BorneMap roles: `public_driver` (implicit), `registered_driver`, `partner`, `admin`.

### Scope Enforcement
The practice of limiting data access to a user's own resources. For partners, enforced by extracting `partner_id` from JWT and applying it as a mandatory filter in middleware. Individual handlers do **not** implement scope checks.

### Authenticated Upgrade Pattern
The UX pattern where public users can browse without login, but a gated action (like favoriting) triggers an auth modal. On successful login, the original action is resumed without user re-triggering.

---

## Services & Architecture

### Service
A backend application that owns specific business responsibilities. BorneMap has five services: Keycloak, Driver Service, Admin Service, Clickstream Service, and Traefik.

### Pragmatic Architecture
The principle of using the minimum number of services that correctly separate responsibilities. No service is added without an ADR proving no existing service can own the responsibility.

### Driver Service
Rust/Actix-web service that owns public station discovery, authenticated driver features (favorites, reviews, profiles). Reads from `inventory` and `gis` schemas. Writes only to `users` schema.

### Admin Service
Rust/Actix-web service that owns partner and station management, manual availability updates, review moderation, and reporting. Writes only to `inventory` schema.

### Clickstream Service
Rust/Actix-web service that owns analytics event ingestion, validation, and persistence. Writes only to `analytics` schema. Public endpoint (no authentication required).

### Traefik
Edge router and reverse proxy. Owns TLS termination, routing, rate limiting, and public port exposure (80 and 443 only). All other services use internal Docker networking.

### Health Check
A mechanism to verify a service is healthy before routing traffic to it or marking it as a dependency. Implemented as HTTP endpoints and Docker Compose health configurations.

---

## Data & Database

### PostgreSQL
Relational database management system version 16. Single source of truth for all data. All services read/write through Postgres exclusively.

### PostGIS
PostgreSQL extension that adds spatial database capabilities (geometry, geography types, spatial indexing). Used for GIS functionality and nearby searches.

### Schema (Database)
A namespace within PostgreSQL. BorneMap uses exactly four: `inventory`, `users`, `gis`, `analytics`.

### Source of Truth
The authoritative owner of an entity's data. Example: `inventory.station` is the source of truth for station data. All other representations (like `gis.station_locations`) are derived.

### inventory Schema
Database schema that owns station, charger, availability, and partner data. Written by Admin Service only. Migrations owned by Admin Service.

### users Schema
Database schema that owns user profiles, favorites, reviews, and partner membership. Written by Driver Service only. Migrations owned by Driver Service.

### gis Schema
Database schema that owns spatial enrichment data (OpenStreetMap data, roads, boundaries, derived station locations). Written by trigger function only. Read by Driver Service for spatial queries.

### analytics Schema
Database schema that owns raw events and event aggregates. Written by Clickstream Service only. Read by Admin Service for reporting.

### Cross-Schema Access
Communication between schemas through defined access rules. Permitted access is documented in constitution section 9. Forbidden access is a Class A violation.

### Trigger (Database)
A database function that executes automatically when a specific event occurs (e.g., INSERT, UPDATE, DELETE). BorneMap uses a trigger on `inventory.station` to synchronize to `gis.station_locations`.

### Spatial Index
A database index optimized for geometric queries (e.g., "find points within 5km"). Required indexes: GIST index on `gis.roads.geom` and `gis.boundaries.geom`.

### GIS Synchronization
The process of keeping `gis.station_locations` synchronized with `inventory.station` via PostgreSQL trigger. Atomic with station writes. Failures logged but do not block the transaction.

### NanoID
A URL-friendly unique string generator (21 characters). All business entities use prefixed NanoIDs (USR-, STN-, etc.). Sequential integers never exposed in public APIs.

### Migration
A versioned database schema change managed by sqlx::migrate!. Services must run migrations on startup before accepting requests.

---

## Frontend & Design

### Design Token
A named semantic variable for a visual value (color, spacing, typography, shadow, radius). Examples: `brand.primary`, `spacing.16`, `font.size.lg`. All visual values must come from tokens; hardcoding forbidden.

### packages/ui
The shared design system package. Contains token definitions, Tailwind base config, and shared components. Consumed by Driver Web, Driver Mobile, and Dashboard.

### Tailwind CSS
A utility-first CSS framework used by web applications. BorneMap extends a base config from `packages/ui/tailwind.config.base.js`.

### React Native
A framework for building native iOS and Android apps using JavaScript/React. Used for Driver Mobile App.

### Plus Jakarta Sans
The primary typography font for Driver Web and Driver Mobile. Selected for high-contrast weight range suitable for map interfaces.

### Inter
The typography font for Dashboard App. Selected for readability in dense data tables.

### RTL (Right-to-Left)
Text and layout direction for languages like Arabic. BorneMap must support RTL correctly on all screens. RTL failures are Class A bugs.

### WCAG 2.1 AA
Web Content Accessibility Guidelines level AA. The minimum accessibility standard for all BorneMap web applications.

### Status Badge
A colored pill/label that indicates state: Available (green), In Use (amber), Maintenance (red). Used on chargers and stations.

---

## Operations & Deployment

### Docker Compose
Orchestration tool for running multiple containers locally and in production. Production uses `docker-compose.prod.yml`. Dev adds pgAdmin.

### Container
An isolated, runnable image of application code and dependencies. BorneMap runs exactly five containers in production: postgres, keycloak, driver-service, admin-service, clickstream-service, traefik.

### Image
A template for creating containers. Built from a Dockerfile. BorneMap images are built on the host during deployment (no image registry).

### Health Check
A test that verifies a service is healthy. Defined in Docker Compose. Dependencies use `condition: service_healthy` to wait for health.

### Runbook
A step-by-step procedure for performing an operational task. Every complex operation must have a documented runbook. Example: deployment-runbook.md.

### Environment Variables
Configuration values injected into services. Stored in host-managed `.env` files (never committed). One env file per service.

### Secret
Sensitive values like database passwords, API keys, OAuth credentials. Stored on the host only (never in images or committed files).

### Deployment
The process of getting new code into production. Always manual following the deployment runbook. No automated deployment.

---

## Analytics & Telemetry

### Event (Analytics)
A recorded user action or system event. Format: `{event_name, session_id, occurred_at, user_id (optional), properties}`. Identified by EVT-... NanoID.

### Event Taxonomy
The canonical list of valid event names and their schemas. Enforced by Clickstream Service. Unknown event names rejected with HTTP 400.

### Clickstream
The stream of user interactions (clicks, views, searches) recorded as analytics events. Direct write to `analytics.raw_events` (no message queue).

### Session
A unique identifier for a user's browsing session. Persists across page/screen reloads. Used to group events by user session.

### Raw Events
The lowest level of analytics data. Individual events as received from clients. Stored in `analytics.raw_events`. Used to compute aggregates.

### Event Aggregates
Pre-computed summaries of raw events. Example: "unique users per day", "searches by location". Computed on a schedule by reporting jobs.

---

## Quality & Testing

### Class A Bug
Blocks correctness, security, or user access. Examples: wrong data returned, auth bypass, RTL broken, spatial index missing. Must be resolved before phase closes.

### Class B Bug
Degrades quality but doesn't block. Example: slow query, missing error message, minor UI misalignment. Must be resolved before target phase closes.

### Class C Bug
Nice-to-have improvement. Example: refactor opportunity, polish, documentation gap. No mandatory phase target.

### Sprint
A time-boxed development cycle (typically 2 weeks). Contains planned tasks, standups, retrospectives, and a done criteria checklist.

### Phase
A larger milestone containing multiple sprints. Example: Phase 1 covers core discovery and partner dashboard.

### Definition of Done
The checklist of criteria that mark work as truly complete. Includes tests passing, no Class A bugs, documentation updated.

### ADR (Architecture Decision Record)
An immutable document recording a major architecture decision and its rationale. Used to explain "why" a system was built a certain way. Never edited; superseded by new ADRs if decisions change.

---

## GIS & Spatial

### OpenStreetMap (OSM)
Free, open-source map data. BorneMap imports OSM data (roads, boundaries, amenities) into the `gis` schema for enrichment and spatial queries.

### Spatial Query
A database query that operates on geometric data. Example: "find stations within 5km of this point". Uses GIST indexes for performance.

### Geometry
A spatial data type representing a point, line, polygon, or collection. PostGIS types: Point, LineString, Polygon, MultiPolygon, etc.

### Road Network
OpenStreetMap roads imported into `gis.roads`. Used for proximity searches (finding nearby stations).

### Administrative Boundary
A political boundary (governorate, region) imported into `gis.boundaries`. Used to enrich station location data.

### Amenity
A point of interest from OpenStreetMap (parking, hospital, restaurant). Imported to `gis.amenity_points`. Optional enrichment for station context.

---

## Common Abbreviations

| Abbr | Full Term |
|------|-----------|
| API | Application Programming Interface |
| ADR | Architecture Decision Record |
| CRUD | Create, Read, Update, Delete |
| E2E | End-to-End Testing |
| EV | Electric Vehicle |
| GIST | Generalized Search Tree (spatial index type) |
| GIS | Geographic Information System |
| HTTP | HyperText Transfer Protocol |
| JWT | JSON Web Token |
| JWKS | JSON Web Key Set |
| OCPP | Open Charge Point Protocol |
| OSM | OpenStreetMap |
| PCI | Payment Card Industry |
| RTL | Right-to-Left |
| SQL | Structured Query Language |
| TLS | Transport Layer Security |
| UX | User Experience |
| WCAG | Web Content Accessibility Guidelines |

---

**Document Version:** 1.0  
**Status:** Active  
**Last Updated:** 2026-06-05
