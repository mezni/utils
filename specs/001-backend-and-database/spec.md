# Feature Specification: API Versioning

**Feature Branch**: `001-api-versioning`

**Created**: 2026-06-08

**Status**: Draft

**Input**: User description: "add version to api endpoints"

## Clarifications

### Session 2026-06-08

- Q: Version Deprecation Timeline — How long should each API version remain supported? → A: 12 months. Each version supported for 12 months after next version release.
- Q: Unversioned Endpoint Support — Should unversioned paths like `/api/stations` work as aliases? → A: No. All endpoints must use explicit versioning (e.g., `/api/v1/stations`). Unversioned paths return 404.
- Q: Version Identifier in Response — Should responses include a version field or header? → A: No. Version is implicit in the URL path only. No additional version field or header required.

## User Scenarios & Testing

### User Story 1 — API Consumer Receives Versioned Endpoints (Priority: P1)

An API consumer (partner, developer) needs to discover and call a specific version of the BorneMap API. They visit the API base URL and see clear, consistent version identifiers on all endpoint paths.

**Why this priority**: This is the core requirement. Without versioning, the API cannot evolve without breaking existing integrations. This is critical for long-term platform stability and MVP-2 transition.

**Independent Test**: Can be fully tested by making HTTP requests to versioned endpoints (e.g., `/api/v1/stations`, `/api/v2/stations`) and verifying they return correct responses, demonstrating that clients can confidently target a specific API version.

**Acceptance Scenarios**:

1. **Given** a client requests `GET /api/v1/stations`, **When** the request is sent with valid parameters, **Then** the response includes all stations with the expected schema for v1
2. **Given** a client requests `/api/v1/health`, **When** the request succeeds, **Then** the response includes the service name and database status
3. **Given** a client is using a versioned endpoint, **When** a new version is released, **Then** the old version continues to work unchanged

---

### User Story 2 — Documentation Shows All Available Versions (Priority: P2)

Developers need to quickly identify which API versions are available and understand what changed between versions. The API documentation clearly lists all active versions.

**Why this priority**: Essential for developer experience and migration planning. Reduces support burden by making API compatibility clear. Enables smooth transition from MVP-1 to MVP-2.

**Independent Test**: Can be fully tested by reading API documentation (e.g., `/docs` or OpenAPI spec) and verifying it lists all available versions (v1, v2, etc.) with compatibility notes.

**Acceptance Scenarios**:

1. **Given** API documentation is accessed, **When** viewing the endpoint reference, **Then** all available versions are listed with their status (active, deprecated)
2. **Given** a developer reads the upgrade guide, **When** they see changes between v1 and v2, **Then** migration steps are clearly explained

---

### User Story 3 — Backward Compatibility is Maintained During Transition (Priority: P2)

When MVP-2 transitions the backend from Python to Rust, the API version persists across the service replacement. Existing v1 clients continue to work without changes.

**Why this priority**: This ensures no client disruption during infrastructure changes. Aligns with the constitution principle of "never breaking what a previous MVP delivered."

**Independent Test**: Can be fully tested by verifying that `/api/v1/stations` works identically before and after the Python→Rust migration, demonstrating backward compatibility.

**Acceptance Scenarios**:

1. **Given** a v1 client is deployed and calling the API, **When** the backend service changes from Python to Rust, **Then** all v1 endpoints continue to work without client changes
2. **Given** v1 schema is locked, **When** v2 is introduced with new fields, **Then** v1 clients do not receive v2 fields unexpectedly

---

### Edge Cases

- What happens when a client requests an invalid version (e.g., `/api/v999/stations`)? → Return HTTP 404 or 400 with clear error message
- How does the health endpoint work with versioning? → `/api/v1/health` and `/api/v2/health` both work, returning consistent service status
- What if a client requests an unversioned endpoint (e.g., `/api/stations`)? → Return HTTP 404. All endpoints require explicit version in URL path.

## Requirements

### Functional Requirements

- **FR-001**: All endpoints MUST be served under a versioned path: `/api/v<number>/<resource>` (e.g., `/api/v1/stations`, `/api/v1/partners`)
- **FR-002**: The `/api/health` endpoint MUST be versioned (e.g., `/api/v1/health`) to enable consistent version checking
- **FR-003**: The API MUST maintain the same response schema for all v1 endpoints to guarantee backward compatibility
- **FR-004**: When a new major version is introduced, the old version MUST continue to function for a minimum of 12 months. Deprecation status must be clearly documented in API reference.
- **FR-005**: OpenAPI/Swagger documentation MUST list all available API versions and their current status (active, deprecated, retired)
- **FR-006**: New endpoints introduced in MVP-2 and beyond MUST follow the same versioning pattern as v1

### Key Entities

- **API Version**: A numeric identifier (1, 2, 3, etc.) representing the API contract generation. Each version has a distinct schema and set of endpoints.
- **Endpoint Path**: HTTP route in the format `/api/v<version>/<resource>`. Example: `/api/v1/stations`, `/api/v2/stations`
- **Schema**: The structure of request and response payloads for a given version. Once published, schemas for a version are immutable.

## Success Criteria

### Measurable Outcomes

- **SC-001**: All 16 Sprint 1.1 endpoints are accessible under `/api/v1/` prefix with no behavioral changes from their current implementation
- **SC-002**: API documentation clearly lists v1 as the current stable version with no ambiguity
- **SC-003**: A new API consumer can identify the correct version to target within 2 minutes by reading the documentation
- **SC-004**: Zero breaking changes to v1 endpoints during MVP-1 and MVP-2 transition — all v1 clients continue to work
- **SC-005**: Health check endpoint (`/api/v1/health`) responds successfully and includes version information

## Assumptions

- Versioning is URL-based (e.g., `/api/v1/...`) rather than header-based, as this is more discoverable and cacheable
- MVP-1 is locked at version 1 (v1). New versions are introduced in MVP-2 and beyond when the backend architecture changes
- Unversioned endpoints (e.g., `/api/stations`) are NOT supported. All clients must use explicit versioned paths (e.g., `/api/v1/stations`). Requests to unversioned paths return HTTP 404.
- API response payloads do not include a version identifier. The version is implicit in the URL path.
- Each released API version is supported for a minimum of 12 months after its successor is released, providing a clear migration window for clients
- API versioning applies to all services — Driver Service, Admin Service, Clickstream Service (from MVP-2 onward) all use the same versioning scheme
- The version number changes only when the API contract breaks; internal implementation changes do not trigger version increments
