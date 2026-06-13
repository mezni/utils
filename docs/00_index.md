# BorneMap Documentation Index

## Version: 1.0
## Purpose: Navigation hub for humans + LLM agents

---

## 🧭 1. SYSTEM OVERVIEW

**BorneMap is a MVP-driven, LLM-executed EV charging platform.**

All development follows strict layers:

**Constitution → MVP → Specs → Execution → Code → Bugs → Fixes**

---

## 🧠 2. CORE PRINCIPLE

**If it is not in Specs, it does not exist.**
**If it is not in MVP, it must not be implemented.**

---

## 📁 3. DOCUMENTATION STRUCTURE MAP

### 3.1 Root Governance

| File | Purpose |
|------|---------|
| [01_constitution.md](./01_constitution.md) | System rules (architecture + constraints) |
| [02_agents.md](./02_agents.md) | OpenCode execution rules |
| [03_implementation-plan.md](./03_implementation-plan.md) | MVP roadmap |

### 3.2 MVP LAYER

Each MVP defines a full vertical slice.

**[Active MVP → docs/mvp/mvp-1-discovery.md](./mvp/mvp-1-discovery.md)** ← CURRENT

- [mvp-1-discovery.md](./mvp/mvp-1-discovery.md)
- [mvp-2-operations.md](./mvp/mvp-2-operations.md)
- [mvp-3-identity.md](./mvp/mvp-3-identity.md)
- [mvp-4-analytics.md](./mvp/mvp-4-analytics.md)
- [mvp-5-hardening.md](./mvp/mvp-5-hardening.md)
- [mvp-6-production.md](./mvp/mvp-6-production.md)

### 3.3 FEATURE SPECS (EXECUTION CONTRACTS)

**Purpose:** Defines exact implementation contracts for OpenCode

**Includes:**
- API contracts
- UX behavior
- Edge cases
- Acceptance criteria

- [station-discovery/](./specs/station-discovery/)
- [nearby-search/](./specs/nearby-search/)
- [station-detail/](./specs/station-detail/)
- [map-interactions/](./specs/map-interactions/)
- [auth-flow/](./specs/auth-flow/)
- [admin-crud/](./specs/admin-crud/)
- [analytics-events/](./specs/analytics-events/)

### 3.4 API CONTRACTS

**Rule:** All APIs MUST follow `/api/v1/*`

- [api/overview.md](./api/overview.md)
- [api/versioning.md](./api/versioning.md)
- [api/driver-service.md](./api/driver-service.md)
- [api/admin-service.md](./api/admin-service.md)
- [api/auth-service.md](./api/auth-service.md)

### 3.5 DATABASE SCHEMA

**Ownership rules:**
- `inventory` → admin-service
- `users` → auth-service
- `gis` → read-only
- `analytics` → append-only

- [schema/overview.md](./schema/overview.md)
- [schema/inventory.md](./schema/inventory.md)
- [schema/gis.md](./schema/gis.md)
- [schema/users.md](./schema/users.md)
- [schema/analytics.md](./schema/analytics.md)

### 3.6 ARCHITECTURE

- [architecture/overview.md](./architecture/overview.md)
- [architecture/frontend.md](./architecture/frontend.md)
- [architecture/backend.md](./architecture/backend.md)
- [architecture/services.md](./architecture/services.md)
- [architecture/data-model.md](./architecture/data-model.md)
- [architecture/network-model.md](./architecture/network-model.md)

### 3.7 DESIGN SYSTEM (UX PRO MAX)

**Purpose:** Defines how the product feels, not how it works

- [design/design-system.md](./design/design-system.md)
- [design/ux-principles.md](./design/ux-principles.md)
- [design/mobile-patterns.md](./design/mobile-patterns.md)
- [design/map-interactions.md](./design/map-interactions.md)
- [design/motion.md](./design/motion.md)
- [design/empty-error-states.md](./design/empty-error-states.md)

### 3.8 EXECUTION CONTROL (CRITICAL FOR OPENCODE)

**Purpose:** Defines what OpenCode is allowed to work on RIGHT NOW

- [execution/active-mvp.md](./execution/active-mvp.md)
- [execution/sprint-backlog.md](./execution/sprint-backlog.md)
- [execution/in-progress.md](./execution/in-progress.md)
- [execution/done-log.md](./execution/done-log.md)
- [execution/release-notes.md](./execution/release-notes.md)

### 3.9 BUG & FEEDBACK LOOP

**Purpose:** Captures runtime failures, prevents repeated LLM mistakes, enforces learning loop

- [bugs/bug-log.md](./bugs/bug-log.md)
- [bugs/known-issues.md](./bugs/known-issues.md)
- [bugs/regression-tests.md](./bugs/regression-tests.md)
- [bugs/fix-history.md](./bugs/fix-history.md)

### 3.10 TESTING

- [testing/strategy.md](./testing/strategy.md)
- [testing/unit.md](./testing/unit.md)
- [testing/integration.md](./testing/integration.md)
- [testing/e2e.md](./testing/e2e.md)
- [testing/map-flow-tests.md](./testing/map-flow-tests.md)

### 3.11 OBSERVABILITY

- [observability/logging.md](./observability/logging.md)
- [observability/metrics.md](./observability/metrics.md)
- [observability/tracing.md](./observability/tracing.md)
- [observability/alerts.md](./observability/alerts.md)

### 🎯 MVP Development

- **[Active MVP](./mvp/active-mvp.md)** - Currently active MVP tracking
- **[Sprint Backlog](./mvp/sprint-backlog.md)** - Sprint-level task tracking
- **[In Progress](./mvp/in-progress.md)** - Currently implemented features
- **[Done Log](./mvp/done-log.md)** - Completed feature log
- **[Release Notes](./mvp/release-notes.md)** - Release announcements and changes

### 📐 MVP Specifications

- **[MVP-1: Discovery](./mvp/mvp-1-discovery.md)** - Station discovery and map functionality
- **[MVP-2: Operations](./mvp/mvp-2-operations.md)** - Operational features
- **[MVP-3: Identity](./mvp/mvp-3-identity.md)** - Authentication and user management
- **[MVP-4: Analytics](./mvp/mvp-4-analytics.md)** - Analytics and insights
- **[MVP-5: Hardening](./mvp/mvp-5-hardening.md)** - Security and performance improvements
- **[MVP-6: Production](./mvp/mvp-6-production.md)** - Production deployment

### 📝 Feature Specifications

- **[Station Discovery](./specs/station-discovery/)** - Station discovery flow
- **[Nearby Search](./specs/nearby-search/)** - Location-based search
- **[Station Detail](./specs/station-detail/)** - Station information view
- **[Map Interactions](./specs/map-interactions/)** - Map user interactions
- **[Auth Flow](./specs/auth-flow/)** - Authentication flow
- **[Admin CRUD](./specs/admin-crud/)** - Admin management operations
- **[Analytics Events](./specs/analytics-events/)** - Analytics event tracking

### 🔌 API Documentation

- **[API Overview](./api/overview.md)** - General API information
- **[API Versioning](./api/versioning.md)** - Versioning strategy
- **[Driver Service](./api/driver-service.md)** - Driver service endpoints
- **[Admin Service](./api/admin-service.md)** - Admin service endpoints
- **[Auth Service](./api/auth-service.md)** - Authentication service endpoints

### 🗄️ Schema Documentation

- **[Schema Overview](./schema/overview.md)** - Database overview
- **[Inventory Schema](./schema/inventory.md)** - Inventory data model
- **[GIS Schema](./schema/gis.md)** - Geographic information system
- **[Users Schema](./schema/users.md)** - User data model
- **[Analytics Schema](./schema/analytics.md)** - Analytics data model

### 🏗️ Architecture Documentation

- **[Architecture Overview](./architecture/overview.md)** - System architecture
- **[Services Architecture](./architecture/services.md)** - Service layer design
- **[Frontend Architecture](./architecture/frontend.md)** - Frontend structure
- **[Backend Architecture](./architecture/backend.md)** - Backend structure
- **[Data Model](./architecture/data-model.md)** - Data relationships
- **[Network Model](./architecture/network-model.md)** - Network topology

### 🎨 Design Documentation

- **[Design System](./design/design-system.md)** - Design tokens and components
- **[UX Principles](./design/ux-principles.md)** - User experience guidelines
- **[Mobile Patterns](./design/mobile-patterns.md)** - Mobile-specific patterns
- **[Map Interactions](./design/map-interactions.md)** - Map UX patterns
- **[Motion](./design/motion.md)** - Motion and animation guidelines
- **[Empty/Error States](./design/empty-error-states.md)** - State handling patterns

### 🚀 Execution Tracking

- **[Active MVP](./execution/active-mvp.md)** - Current sprint priorities
- **[Sprint Backlog](./execution/sprint-backlog.md)** - Sprint task list
- **[In Progress](./execution/in-progress.md)** - Currently working on
- **[Done Log](./execution/done-log.md)** - Completed items
- **[Release Notes](./execution/release-notes.md)** - Release announcements

### 🐛 Bug Tracking

- **[Bug Log](./bugs/bug-log.md)** - Current bug list
- **[Known Issues](./bugs/known-issues.md)** - Documented issues
- **[Regression Tests](./bugs/regression-tests.md)** - Test cases for regressions
- **[Fix History](./bugs/fix-history.md)** - Past bug fixes

### 📋 Architecture Decisions

- **[ADR-0001: Initial Architecture](./adr/0001-initial-architecture.md)** - Core architecture decisions
- **[ADR-0002: API Versioning](./adr/0002-api-versioning.md)** - API versioning strategy
- **[ADR-0003: Auth Service](./adr/0003-auth-service-introduction.md)** - Authentication architecture
- **[ADR-0004: Frontend Split](./adr/0004-frontend-split.md)** - Frontend structure

### 🧪 Testing Documentation

- **[Testing Strategy](./testing/strategy.md)** - Overall testing approach
- **[Unit Tests](./testing/unit.md)** - Unit testing guidelines
- **[Integration Tests](./testing/integration.md)** - Integration testing approach
- **[E2E Tests](./testing/e2e.md)** - End-to-end testing
- **[Map Flow Tests](./testing/map-flow-tests.md)** - Map-specific test cases

### 📊 Observability

- **[Logging](./observability/logging.md)** - Logging strategy
- **[Metrics](./observability/metrics.md)** - Metrics collection
- **[Tracing](./observability/tracing.md)** - Distributed tracing
- **[Alerts](./observability/alerts.md)** - Alert configuration

---

## 🚀 4. MVP QUICK ACCESS (CURRENT WORK)

### 🔵 ACTIVE MVP

**MVP-1 → Discovery Core**

**Includes:**
- Map view
- Station markers
- Nearby search
- Station detail
- Basic analytics events

**Forbidden:**
- auth
- dashboard
- admin
- partner flows

---

## 🧩 5. LLM EXECUTION FLOW

**When OpenCode runs:**

1. Read Constitution
2. Check active MVP
3. Read SpecKit feature
4. Validate API contract
5. Confirm UX rules
6. Implement only allowed scope
7. Log changes
8. Update bug log if needed

---

## ⚠️ 6. SYSTEM GUARANTEE

This documentation system guarantees:

- ✅ No feature drift across MVPs
- ✅ No architectural corruption
- ✅ Strict LLM execution safety
- ✅ Predictable frontend + backend evolution
- ✅ Traceable bugs and fixes

---

## 🧠 7. FINAL RULE

**The documentation is the system.**
**The code is just its execution.**

---

## Quick Links

- [Active MVP](./mvp/mvp-1-discovery.md)
- [API Documentation](./api/overview.md)
- [Latest Architecture Decisions](./adr/)
- [Testing Strategy](./testing/strategy.md)

## Contribute

All documentation should be maintained in accordance with the project constitution and implementation plan.
