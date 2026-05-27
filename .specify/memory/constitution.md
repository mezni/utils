<!--
  Sync Impact Report
  Version Change: (template) → 1.0.0
  Modified Principles: N/A (initial fill from template)
  Added Sections:
    - I. Validation-First Spatial Platform
    - II. Rust-First Backend
    - III. Access Isolation & Role Boundaries
    - IV. Spatial UX Excellence
    - V. Controlled Operational Complexity
    - System Access Architecture (Section 2)
    - Platform Topology (Section 3)
  Removed Sections: None
  Templates Requiring Updates:
    - .specify/templates/plan-template.md: ✅ updated (generic placeholder — gates filled per context)
    - .specify/templates/spec-template.md: ✅ updated (no constitution-specific changes needed)
    - .specify/templates/tasks-template.md: ✅ updated (no constitution-specific changes needed)
    - .specify/templates/checklist-template.md: ✅ updated (no constitution-specific changes needed)
  Deferred Items: None
  Follow-up TODOs: Create README.md with project overview referencing constitution
-->
# BorneMap Constitution

## Core Principles

### I. Validation-First Spatial Platform
BorneMap's primary objective is fast product validation through rapid iteration.
Every feature MUST be driven by measurable validation criteria before implementation.
The platform validates the full EV charging ecosystem through four coordinated actor
profiles: Public users, Drivers, Partners, and Administrators. Spatial discovery UX
MUST be treated as the core differentiator and validated continuously.

Rationale: Quick market validation of EV charging infrastructure discovery requires
ruthless prioritization of learnings over perfection. No feature ships without a
defined validation hypothesis.

### II. Rust-First Backend
All backend services MUST be implemented in Rust (stable toolchain) using the Actix
Web framework with Tokio async runtime. Actix Web is the mandatory backend framework.
Framework replacement is prohibited during validation unless a measurable blocker is
proven and documented.

Database access MUST use SQLx for compile-time checked queries. The backend engine
MUST provide high-performance asynchronous spatial API with deterministic concurrency
and production-grade request throughput.

Rationale: Actix Web provides mature Rust web ecosystem stability, high-performance
async request handling, strong middleware composition, and excellent production
throughput characteristics.

### III. Access Isolation & Role Boundaries
The authorization model MUST enforce strict role boundaries with no scope leakage.
Four roles are defined:
- **public**: Read-only public station discovery
- **driver**: Personalized user actions (favorites, reviews, search)
- **partner**: Ownership-scoped infrastructure management
- **admin**: Global unrestricted platform administration

Partners SHALL only access resources explicitly bound to their ownership scope.
Partner JWT tokens MUST include `partner_id` and `owned_station_scope` claims.
All API endpoints MUST validate role claims before returning responses.

Rationale: Ownership isolation is critical for a multi-tenant spatial platform.
Role boundaries prevent data leakage between competing partners and protect
user privacy.

### IV. Spatial UX Excellence
The platform delivers three coordinated applications:
1. **Driver Mobile** (React Native, Expo Go, TypeScript): Map exploration, station
   discovery, favorites, reviews, search and filtering.
2. **Partner Mobile** (React Native, Expo Go, TypeScript): Partner dashboard, owned
   station management, metadata updates, operational visibility, basic analytics.
3. **Admin Web Portal** (React, Vite, Tailwind CSS, shadcn/ui, Leaflet): Full station
   management, partner management, data moderation, spatial system oversight.

Each application MUST prioritize spatial interaction quality and responsive design.

Rationale: Different user profiles require tailored interfaces. A single monolithic
UI cannot serve drivers on-the-go, partners managing assets, and administrators
overseeing the platform simultaneously.

### V. Controlled Operational Complexity
Architecture MUST prioritize simplicity and ownership isolation. Every new abstraction
MUST justify itself against a concrete current need (YAGNI). The backend API MUST
maintain clean separation between spatial data processing, business logic, and
authorization concerns.

Complexity MUST be justified in writing. Simpler alternatives MUST be explicitly
rejected with documented reasoning. The platform SHOULD prefer fewer moving parts
over architectural elegance.

Rationale: Rapid validation is incompatible with over-engineered systems. Controlled
complexity ensures the platform can pivot quickly based on market feedback.

## System Access Architecture

### Role Matrix
| Role | Scope |
|------|-------|
| public | Read-only public station discovery |
| driver | Personalized user actions |
| partner | Ownership-scoped infrastructure management |
| admin | Global unrestricted platform administration |

### JWT Claim Specification
JWT role claims MUST support these values: `public`, `driver`, `partner`, `admin`.
Partner tokens MUST additionally include:
- `partner_id`: Identifier binding the token to a specific partner entity
- `owned_station_scope`: List of station identifiers the partner is authorized to
  manage

### Access Isolation Enforcement
- Partners SHALL only access resources explicitly bound to their ownership scope
- All endpoints MUST validate:
  1. Token validity and expiration
  2. Role permission for the requested operation
  3. Resource ownership scoping (for partner role)
- Admin role bypasses ownership scope checks but MUST log all admin mutations

## Platform Topology

### Driver Mobile Application
- **Audience**: Public users + registered drivers
- **Framework**: React Native + Expo Go + TypeScript
- **Responsibilities**: Map exploration, station discovery, favorites, reviews,
  search and filtering

### Partner Mobile Application
- **Audience**: Infrastructure partners
- **Framework**: React Native + Expo Go + TypeScript
- **Responsibilities**: Partner dashboard, owned station management, metadata
  updates, operational visibility management, basic analytics

### Admin Web Portal
- **Audience**: Platform administrators
- **Framework**: React + Vite + Tailwind CSS + shadcn/ui + Leaflet
- **Responsibilities**: Full station management, partner management, data
  moderation, spatial system oversight, platform control panel

## Governance

This constitution supersedes all other project practices. Amendments require:

1. **Documentation**: Proposed change MUST be written with rationale
2. **Approval**: Amendment MUST be reviewed and approved by project maintainers
3. **Version Bump**: Constitution version MUST follow semantic versioning:
   - MAJOR: Backward-incompatible governance changes, principle removals/redefinitions
   - MINOR: New principles or materially expanded guidance
   - PATCH: Clarifications, wording refinements, typo fixes
4. **Migration Plan**: Material changes MUST include transition guidance

Compliance verification is required for all PRs and feature implementations.
Any deviation from constitution principles MUST be justified in the
Complexity Tracking section of the implementation plan.

**Version**: 1.0.0 | **Ratified**: 2026-05-27 | **Last Amended**: 2026-05-27
