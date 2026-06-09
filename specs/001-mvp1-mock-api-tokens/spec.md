# Feature Specification: MVP-1 Foundation Setup

**Feature Branch**: `001-mvp1-mock-api-tokens`

**Created**: 2026-06-08

**Status**: Draft

**Input**: User description: "MVP-1 Sprint 1.1 — Mock API and Design System Foundation. json-server with seeded data under /api prefix. Design tokens package consumable by all apps. pnpm workspace with root scripts."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Station Data API (Priority: P1)

As a public driver, I want EV charging station data to be accessible so that I can discover stations near me through a web or mobile app.

**Why this priority**: Station data availability is the foundation for every downstream capability. Without it, neither the driver apps nor the dashboard can function.

**Independent Test**: Start the mock server, request station list, and confirm 15 Tunisian stations with coordinates, status, and partner information are returned. All apps can be developed against this data.

**Acceptance Scenarios**:

1. **Given** the mock server is running, **When** a client requests all stations, **Then** exactly 15 stations are returned with coordinates, partner association, and operational status.
2. **Given** the mock server is running, **When** a client requests stations filtered by partner, **Then** only stations belonging to that partner are returned.
3. **Given** the mock server is running, **When** a client requests chargers filtered by station, **Then** only chargers belonging to that station are returned.
4. **Given** the mock server is running, **When** a driver app requests nearby stations, **Then** all stations are returned and filtering is applied on the client side.

---

### User Story 2 - Design System Foundation (Priority: P1)

As a developer building the driver web, driver mobile, and dashboard applications, I want a shared design token package and monorepo workspace so that all apps maintain visual consistency from the start.

**Why this priority**: Principle IX of the constitution requires that all three applications share the same design token foundation with no hardcoded visual values. The workspace must be set up before app development begins.

**Independent Test**: Import color tokens from the UI package, verify brand primary resolves to `#007943` and the Tailwind config picks up the full token set. A simple component renders with correct brand styling in both web and mobile targets.

**Acceptance Scenarios**:

1. **Given** the UI package is initialized, **When** any token file is imported, **Then** it exports its values without TypeScript errors.
2. **Given** the design token files are complete, **When** `colors.ts` is updated, **Then** `native.ts` is updated in the same commit with equivalent React Native values.
3. **Given** the Tailwind config extends the design tokens, **When** a class references `brand-primary`, **Then** it resolves to `#007943`.
4. **Given** the pnpm workspace is configured, **When** any app runs `pnpm dev`, **Then** it starts successfully with access to the UI package.

---

### User Story 3 - Workspace & Developer Experience (Priority: P2)

As a developer, I want a unified monorepo workspace with consistent scripts and dependencies so that I can run the mock API and all applications with a single command.

**Why this priority**: Developer efficiency matters, but the core value is delivered by Stories 1 and 2. The workspace orchestration is enabling infrastructure.

**Independent Test**: Run `pnpm dev` from the root, confirm that json-server and the dashboard app start concurrently. Stop both with a single signal.

**Acceptance Scenarios**:

1. **Given** the pnpm workspace is configured, **When** I run `pnpm mock`, **Then** json-server starts on port 3001 and serves the mock API.
2. **Given** the root package.json has all scripts, **When** I run `pnpm dev`, **Then** both json-server and the dashboard app start concurrently.

### Edge Cases

- What happens when the mock API is requested with an unknown filter parameter? json-server ignores unrecognized query parameters and returns the full resource.
- What happens when a resource does not exist (e.g., `/api/stations/999`)? json-server returns a 404 empty response.
- What happens when the mock server is unreachable? Apps should display an appropriate error state (handled at the app level in subsequent sprints).

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The system MUST serve a list of partners (exactly 3) at a discoverable endpoint.
- **FR-002**: The system MUST serve a list of stations (exactly 15) with Tunisian city coordinates, each linked to a partner.
- **FR-003**: The system MUST serve a list of chargers (exactly 24) each linked to a station.
- **FR-004**: The system MUST support filtering stations by partner identifier.
- **FR-005**: The system MUST support filtering chargers by station identifier.
- **FR-006**: The system MUST serve all endpoints under an `/api` prefix.
- **FR-007**: The system MUST serve a stations endpoint that returns all stations for client-side nearby filtering.
- **FR-008**: The system MUST provide a complete set of color tokens covering brand, surface, text, border, status, and neutral scales.
- **FR-009**: The system MUST provide typography tokens covering font families for driver apps (Plus Jakarta Sans, Inter) and dashboard (Inter).
- **FR-010**: The system MUST provide spacing tokens on a 4px base scale: 4, 8, 12, 16, 20, 24, 32, 40, 48, 64, 80, 96.
- **FR-011**: The system MUST provide radius tokens: sm(4), md(8), lg(12), xl(16), 2xl(20), 3xl(24), full(9999).
- **FR-012**: The system MUST provide shadow tokens: card, panel, float, pin.
- **FR-013**: The system MUST provide equivalent design tokens in a format consumable by React Native (`native.ts`).
- **FR-014**: The system MUST provide a Tailwind config that extends all design tokens for use in web applications.
- **FR-015**: The pnpm workspace MUST include all apps under `source/apps/*` and all packages under `source/packages/*`.
- **FR-016**: The root package.json MUST include scripts for `mock` (json-server), `dev:dashboard`, `dev:web`, `dev:mobile`, and `dev` (mock + dashboard concurrently).

### Key Entities

- **Partner**: An organization that owns and operates EV charging stations. Key attributes: name, identifier.
- **Station**: A physical EV charging location with coordinates, address, and status. Belongs to exactly one partner. Key attributes: name, coordinates, address, status, partner association.
- **Charger**: An individual charging unit at a station. Belongs to exactly one station. Key attributes: identifier, type, power rating, status.
- **Design Token**: A named visual value (color, typography, spacing, radius, shadow) that forms the shared design language across all applications.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A developer can start the mock server with a single command and verify exactly 15 stations, 3 partners, and 24 chargers are served through the API.
- **SC-002**: All API endpoints are reachable under the `/api` prefix and support resource filtering by parent identifier.
- **SC-003**: A developer can import any design token file and verify all values are valid and correctly defined.
- **SC-004**: The brand primary color (`#007943`) is correctly applied when used in any application consuming the shared design tokens.
- **SC-005**: A developer can start the full development environment, including the data API and dashboard application, with a single command.
- **SC-006**: The design token set for mobile platforms contains every token present in the primary web set, verified by identical value references.

## Assumptions

- The mock server relies on json-server and requires no custom backend logic or database.
- Station coordinates are static in the seed data; no spatial queries or geographic filtering is performed server-side.
- Nearby station filtering is handled entirely on the client side within the driver applications.
- All three apps (Dashboard, Driver Web, Driver Mobile) are developed in separate, subsequent sprints that build on this foundation.
- The pnpm package manager is used exclusively for workspace management.
- No authentication, user accounts, or session management is needed for MVP-1.
- Design tokens are the single source of truth for visual values; no hardcoded styling is introduced in any application code.
