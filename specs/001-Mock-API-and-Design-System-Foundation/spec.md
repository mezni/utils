# Feature Specification: Mock API and Design System Foundation

**Feature Branch**: `001-mock-api-and-design-system-foundation`

**Created**: 2026-06-09

**Status**: Draft

**Input**: User description: "Sprint 1.1 — Mock API and Design System Foundation"

## User Scenarios & Testing

### User Story 1 - Developer accesses mock API for all resources (Priority: P1)

As a developer working on frontend applications, I need a working REST API serving partners, stations, chargers, and availability data under the /api prefix so that I can build and test all three apps (Dashboard, Driver Web, Driver Mobile) against realistic data.

**Why this priority**: All frontend development depends on the API being available. Without it, no app can function.

**Independent Test**: Can be fully tested by starting the mock server and verifying all four resources are reachable under the /api prefix with correct response structures.

**Acceptance Scenarios**:

1. **Given** the mock server is running, **When** I send GET /api/partners, **Then** I receive a JSON array of 3 partner objects each containing id, name, type, is_verified, is_live, is_active, created_at, created_by, updated_at, updated_by
2. **Given** the mock server is running, **When** I send GET /api/stations?partner_id=PRT001, **Then** I receive only stations belonging to that partner
3. **Given** the mock server is running, **When** I send GET /api/chargers?station_id=STN001, **Then** I receive only chargers belonging to that station
4. **Given** the mock server is running, **When** I send GET /api/station_availability, **Then** I receive an array of availability records with id, station_id, status, updated_by, updated_at

---

### User Story 2 - Frontend developer uses shared design tokens (Priority: P2)

As a frontend developer, I need a central design token package so that all three apps share consistent colors, typography, spacing, and shadows without hardcoding visual values.

**Why this priority**: Design tokens are consumed by all apps but the API is a prerequisite for any app functionality. Tokens are needed before screen development in Sprint 1.2+.

**Independent Test**: Can be tested by importing the token package and verifying color values resolve correctly (e.g., brand.primary = #007943).

**Acceptance Scenarios**:

1. **Given** the UI package is initialized, **When** I import colors.ts, **Then** all color tokens (brand.primary, brand.primaryDark, brand.sageLight, surface.*, text.*, border.*, status.*) are exported with correct hex values
2. **Given** the UI package is built, **When** I import typography.ts, **Then** font family, size, and weight tokens are available
3. **Given** native.ts is created, **When** I import it in a React Native project, **Then** all tokens match colors.ts values

---

### User Story 3 - Developer runs the full workspace with one command (Priority: P3)

As a developer, I need a pnpm workspace configured so that I can start any app or the mock server with simple commands from the project root.

**Why this priority**: Developer experience improvement that reduces friction but does not block any feature work.

**Independent Test**: Can be tested by running `pnpm mock`, `pnpm dev:dashboard`, `pnpm dev:web`, `pnpm dev:mobile` from the project root.

**Acceptance Scenarios**:

1. **Given** the workspace is configured, **When** I run `pnpm mock` from the project root, **Then** the json-server starts on port 3001 with all resources available
2. **Given** the workspace is configured, **When** I run `pnpm dev`, **Then** all available dev commands list without error

---

### Edge Cases

- What happens when json-server is started with an empty or malformed db.json? Server should report a JSON parse error on startup.
- What happens when a filter query targets a non-existent partner_id? Returns empty array, not an error.
- What happens when required token fields are missing? TypeScript compilation should fail with clear type errors.
- What happens when a developer imports an undefined token key? Should produce a TypeScript compile-time error.

## Requirements

### Functional Requirements

- **FR-001**: Mock server MUST serve all four resources (partners, stations, chargers, station_availability) under the /api prefix
- **FR-002**: Partner objects MUST include fields: id, name, type, is_verified, is_live, is_active, created_at, created_by, updated_at, updated_by
- **FR-003**: Station objects MUST include fields: id, partner_id, name, address, latitude, longitude, created_at, created_by, updated_at, updated_by
- **FR-004**: Charger objects MUST include fields: id, station_id, connector_type, power_kw, status, created_at, created_by, updated_at, updated_by
- **FR-005**: Station_availability objects MUST include fields: id, station_id, status, updated_by, updated_at
- **FR-006**: Seeded data MUST include 3 partners in distinct flag states, 15 stations across Tunisian cities, 24 chargers, and 15 availability records
- **FR-007**: Design token package MUST export colors, typography, spacing, radius, and shadow values
- **FR-008**: Color tokens MUST include all brand.*, surface.*, text.*, border.*, and status.* tokens as defined in the design system
- **FR-009**: native.ts MUST export the same values as colors.ts for React Native consumption
- **FR-010**: tailwind.config.base.js MUST extend the shared tokens for web apps
- **FR-011**: pnpm workspace MUST list source/apps/* and source/packages/* as workspace entries
- **FR-012**: Root package.json MUST include scripts: mock, dev:dashboard, dev:web, dev:mobile, dev
- **FR-013**: Stations MUST support filter by partner_id via query parameter
- **FR-014**: Chargers MUST support filter by station_id via query parameter

### Key Entities

- **Partner**: Organization or individual operating EV stations. Has type (business/personal), three operational flags (is_verified, is_live, is_active), and full audit trail.
- **Station**: A physical EV charging location. Belongs to a partner. Has coordinates, address, and audit trail.
- **Charger**: An individual EV charging point at a station. Has connector type (type2/ccs/chademo/type1), power rating, and status (available/in_use/maintenance/offline).
- **Station Availability**: Append-only log of station-level availability status (available/partial/unavailable). Current status determined by most recent record per station.

## Success Criteria

### Measurable Outcomes

- **SC-001**: Developer can start the mock server with `pnpm mock` and all four resources are reachable at /api/partners, /api/stations, /api/chargers, /api/station_availability within 10 seconds
- **SC-002**: Filter queries return correct filtered results — GET /api/stations?partner_id=X returns only that partner's stations
- **SC-003**: Design token files compile without TypeScript errors when imported
- **SC-004**: All token values in native.ts match corresponding values in colors.ts — verifiable by diff
- **SC-005**: tailwind.config.base.js resolves brand.primary to #007943 when loaded in a Tailwind context

## Assumptions

- Developers have Node.js 18+ and pnpm installed
- json-server version is determined at initialization time and compatible with routes.json rewrites
- The three partner seed states cover all validation scenarios: (1) verified + live + active, (2) verified + not live + active, (3) not verified + not live + active
- Token values are static and do not change after this sprint — any change is a new sprint
- The pnpm workspace pattern (source/apps/*, source/packages/*) is used consistently
- Expo SDK 54 tooling is not required in this sprint (it comes in Sprint 1.5)
