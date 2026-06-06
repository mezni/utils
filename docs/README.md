# BorneMap Platform Documentation

BorneMap is an EV station discovery and management system for Tunisia, supporting four user types: Public Drivers, Registered Drivers, Partners, and Admins.

## Quick Navigation

### [Core Documentation](core/README.md)
- [Constitution](core/constitution.md) — Platform principles and non-negotiable rules
- [Project Scope](core/scope.md) — What's included and deferred
- [Glossary](core/glossary.md) — Terminology and entity types
- [Implementation Plan](core/implementation-plan.md) — Phases and deliverables
- [Decisions Log](core/decisions.md) — Small decisions and tracking

### [Architecture](architecture/README.md)
- [System Overview](architecture/overview.md) — High-level system design
- [Service Responsibilities](architecture/services.md) — Backend services and boundaries
- [Data Architecture](architecture/data.md) — Schemas, cross-schema rules, sync mechanisms
- [Deployment](architecture/deployment.md) — Docker Compose, Docker Health Checks
- [GIS Synchronization](architecture/gis.md) — PostGIS triggers and spatial indexing

### [Domain Knowledge](domain/README.md)
- [Data Model](domain/model.md) — Entity types, relationships
- [Business Rules](domain/rules.md) — Validation and constraints
- [User Roles & Access](domain/access-model.md) — Public Driver, Registered Driver, Partner, Admin

### [APIs](api/README.md)
- [Driver Service API](api/driver.md) — Discovery, profile, favorites, reviews
- [Admin Service API](api/admin.md) — Partner, station, charger, reporting
- [Clickstream Service API](api/analytics.md) — Event ingestion, taxonomy
- [Integration Points](api/integration.md) — Service-to-service communication

### [Design System](ui/README.md)
- [Token Foundation](ui/design-tokens.md) — Colors, typography, spacing, shadows
- [Components](ui/components.md) — Shared, driver-specific, dashboard-specific
- [Web Delivery](ui/web-tokens.md) — Tailwind CSS configuration
- [Native Delivery](ui/native-tokens.md) — React Native StyleSheet
- [Accessibility](ui/accessibility.md) — WCAG AA, Arabic RTL, language support

### [Data Schemas](data/README.md)
- [Inventory Schema](data/inventory.md) — Partner, Station, Charger
- [Users Schema](data/users.md) — User accounts, profiles, relationships
- [GIS Schema](data/gis.md) — OpenStreetMap, spatial enrichment
- [Analytics Schema](data/analytics.md) — Event storage and aggregates

### [Authentication & Authorization](auth/README.md)
- [Authentication](auth/authentication.md) — Keycloak ownership, token flow
- [Authorization](auth/authorization.md) — Role enforcement, partner scoping
- [Token Management](auth/tokens.md) — Storage rules, refresh, JWKS caching

### [Operations](ops/README.md)
- [Runbook](ops/runbook.md) — Common operational procedures
- [Environment Configuration](ops/environment.md) — Variables, secrets, host setup
- [Deployment Runbook](ops/deployment-runbook.md) — Manual deployment steps
- [Monitoring & Health](ops/monitoring.md) — Health checks, logging, alerts
- [Backup & Recovery](ops/backup.md) — Database backups, disaster recovery

### [Quality Assurance](quality/README.md)
- [Testing Strategy](quality/testing.md) — Unit, integration, E2E coverage
- [Security](quality/security.md) — Best practices, secret handling, access control
- [Release Checklist](quality/release-checklist.md) — Pre-deployment verification

### [Project Management](core/project/README.md)
- [Backlog](core/project/backlog.md) — All pending work
- [Bug Tracking](core/project/bugs.md) — Reported issues, classification
- [Roadmap](core/project/roadmap.md) — Phases and timeline
- [Sprints](core/project/sprints/) — Planning and retrospectives

### [Architecture Decision Records](adr/README.md)
All major decisions recorded as immutable ADRs. See [ADR Index](adr/README.md).

### [Guides](guides/README.md)
- [Onboarding](guides/onboarding.md) — New team member setup
- [Contributing](guides/contribution.md) — Development workflow
- [Feature Flags](guides/feature-flags.md) — Feature toggle usage
- [Event Taxonomy](guides/event-taxonomy.md) — Analytics event schema

---

## Key Principles at a Glance

1. **Pragmatic Architecture** — Minimum services, clear responsibilities
2. **Single Source of Truth** — Every entity has exactly one authoritative owner
3. **Simple Operations** — One person can operate the platform
4. **Domain Separation by Schema** — Business, GIS, users, and analytics separate
5. **Build for Current Scale** — No premature optimization
6. **Public Access First** — Anonymous browsing always works
7. **RTL & Arabic Built-In** — Not an afterthought
8. **Visual Consistency** — Tokens define all visual values

---

## Platform Scope

**Included:**
- Public and authenticated station discovery
- Map-based interface with markers and filtering
- Favorites and reviews
- Partner station management
- Admin platform control
- GIS data enrichment
- Clickstream analytics

**Deferred (Out of Scope):**
- OCPP and charging sessions
- Payment and billing
- Routing and navigation
- Real-time availability (OCPP-driven)
- Push notifications

See [Project Scope](core/scope.md) for details.

---

## Services Overview

| Service | Framework | Responsibility |
|---------|-----------|---|
| **Keycloak** | Auth Server | Authentication, JWT, roles, partner_id claim |
| **Driver Service** | Rust/Actix | Public/auth discovery, profiles, favorites, reviews |
| **Admin Service** | Rust/Actix | Partners, stations, chargers, reporting, moderation |
| **Clickstream Service** | Rust/Actix | Analytics event ingestion, validation, persistence |
| **Traefik** | Edge Router | TLS, routing, rate limiting, public port exposure |

---

## Frontend Applications

| App | Tech | Purpose |
|-----|------|---------|
| **Driver Web** | React + Vite | Map-centric public/authenticated driver experience |
| **Driver Mobile** | React Native + Expo | iOS/Android driver with bottom sheet pattern |
| **Dashboard** | React + Vite | Single app for partner and admin views (role-switched) |

All three share design tokens from `packages/ui`.

---

## Database: PostgreSQL 16 + PostGIS

**Four Schemas:**
- **inventory** — Stations, chargers, availability (Admin Service writes)
- **users** — Profiles, favorites, reviews (Driver Service writes)
- **gis** — Spatial enrichment, OpenStreetMap data (trigger-driven writes)
- **analytics** — Events and aggregates (Clickstream Service writes)

Cross-schema access is strictly controlled. See [Data Architecture](architecture/data.md).

---

## Getting Started

1. **New team member?** → [Onboarding Guide](guides/onboarding.md)
2. **Want to contribute?** → [Contributing Guide](guides/contribution.md)
3. **Need to deploy?** → [Deployment Runbook](ops/deployment-runbook.md)
4. **Troubleshooting?** → [Runbook](ops/runbook.md)
5. **Understanding the system?** → [Architecture Overview](architecture/overview.md)

---

## Non-Negotiable Rules (Class A)

These must never be violated:

- **inventory.station** is the source of truth for stations
- **Public access** never requires login
- **Tokens** never stored in localStorage or AsyncStorage
- **Arabic RTL** must work on every screen
- **Only Traefik** exposes public ports
- **Keycloak** owns all authentication
- **No additional services** without an approved ADR

See [Constitution](core/constitution.md) for the complete list.

---

## Questions?

- Browse the relevant section above
- Check the [Glossary](core/glossary.md) for terminology
- Review past decisions in [ADRs](adr/README.md)
- Check [Runbook](ops/runbook.md) for common tasks
