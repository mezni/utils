# Feature Specification: Cargo Workspace and Shared Crates

**Feature Branch**: `007-cargo-workspace-and-shared-crates`

**Created**: 2026-06-09

**Status**: Draft

**Input**: Sprint 2.1 — Initialize the Rust workspace, build the ev-core crate (NanoID generation, shared enums), and build the ev-db crate (PgPool wrapper, pagination structs).

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Cargo Workspace Builds Successfully (Priority: P1)

A developer clones the repository and runs `cargo build --all`. The Rust workspace at the repository root compiles both ev-core and ev-db crates without errors or warnings. This is the foundation for all MVP-2 development — without a clean build, no further Rust work is possible.

**Why this priority**: Every subsequent MVP-2 sprint depends on the workspace and shared crates. If the workspace doesn't build, nothing else works.

**Independent Test**: Run `cargo build --all` from the repository root on a clean checkout. The command exits with status 0 and produces zero compiler warnings.

**Acceptance Scenarios**:

1. **Given** the repository is cloned and Rust toolchain is installed, **When** `cargo build --all` is run from the repository root, **Then** the command exits successfully with zero errors and zero warnings.
2. **Given** a clean build, **When** `cargo test --all` is run, **Then** all unit tests in both crates pass.
3. **Given** the workspace configuration, **When** a developer lists workspace members, **Then** both `ev-core` and `ev-db` are present.

---

### User Story 2 — Shared Enums and NanoID Generation (Priority: P2)

A developer needs to generate unique, URL-safe identifiers for partners, stations, and chargers. They also need shared enum types (connector type, charger status, partner type) that are consistent across all Rust services. They import `ev-core` and get both — NanoID generation with configurable prefix and length, plus all canonical enums matching the MVP-1 data model.

**Why this priority**: Shared enums prevent type drift between services (e.g., Driver Service and Admin Service must agree on what "maintenance" means). NanoID provides human-friendly IDs that are URL-safe and sortable — a deliberate departure from MVP-1's sequential strings.

**Independent Test**: A test program imports ev-core, generates 1000 NanoIDs with a "PRT" prefix, and verifies all are unique and match the expected format. Enum values round-trip through serialization and deserialization without data loss.

**Acceptance Scenarios**:

1. **Given** the ev-core crate, **When** a NanoID is generated with prefix "PRT" and length 8, **Then** the result matches the pattern `PRT[A-Za-z0-9]{8}`.
2. **Given** the ev-core crate, **When** 1000 NanoIDs are generated, **Then** all are unique (no collisions).
3. **Given** the ev-core crate, **When** each enum type (ConnectorType, ChargerStatus, PartnerType) is serialized to a string and deserialized back, **Then** the round-trip preserves the original value.
4. **Given** the ev-core crate, **When** a developer inspects the public API, **Then** all enums from Sprint 1.1's data model are present (ConnectorType, ChargerStatus, PartnerType, StationStatus).

---

### User Story 3 — Database Pool and Pagination Utilities (Priority: P2)

A developer building a service with database access needs a shared, idiomatic way to create a PostgreSQL connection pool and to paginate query results. They import `ev-db`, pass a connection string, and get a configured `PgPool`. They use the `Paginated` struct to wrap query results with total count and page metadata.

**Why this priority**: Every service (Driver Service, Admin Service, Clickstream Service) needs a database pool. Every list endpoint needs pagination. Sharing this code prevents duplication and ensures consistent behavior.

**Independent Test**: A test program initializes a PgPool from a connection string, runs a simple query against a test database, and returns results wrapped in a Paginated struct with correct total count and page metadata.

**Acceptance Scenarios**:

1. **Given** the ev-db crate, **When** a PgPool is initialized from a valid connection string, **Then** the pool connects and can run a simple query (`SELECT 1`).
2. **Given** the ev-db crate, **When** query results are wrapped in a Paginated struct with page size 20 and total 100 items, **Then** the struct reports `total: 100`, `page: 1`, `page_size: 20`, `total_pages: 5`.
3. **Given** the ev-db crate, **When** a connection string is missing or invalid, **Then** pool initialization returns a clear error message identifying the missing or malformed field.

---

### Edge Cases

- NanoID collision — statistically improbable at length 8 but should never occur within the platform's lifetime. The test verifies 1000 generations with zero collisions. Production length defaults to 12.
- Empty prefix for NanoID — allowed; the ID is purely random characters.
- Database connection string missing required fields (host, port, database name) — each missing field produces a distinct, actionable error message.
- Paginated struct with zero total items — reports `total: 0`, `page: 1`, `page_size: 20`, `total_pages: 0`.
- Paginated struct with page number exceeding total pages — allowed; returns empty data array with correct total metadata.
- Enum serialization of unknown string values — returns a controlled error rather than panicking.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The Cargo workspace MUST be defined at the repository root with workspace members `ev-core` and `ev-db`.
- **FR-002**: The workspace MUST build with `cargo build --all` producing zero errors and zero warnings.
- **FR-003**: ev-core MUST provide a function to generate NanoID strings with configurable prefix (alphanumeric string, may be empty) and length (positive integer).
- **FR-004**: ev-core MUST guarantee that 1000 sequentially generated NanoIDs with the same prefix and length are unique (zero collisions).
- **FR-005**: ev-core MUST define the following enum types with serialization/deserialization support:
  - ConnectorType (values: Type2, Type3, CCS, CHAdeMO)
  - ChargerStatus (values: Available, InUse, Maintenance, Offline)
  - PartnerType (values: Business, Personal)
  - StationStatus (values: Available, Partial, Unavailable)
- **FR-006**: Enum deserialization of unknown string values MUST return a controlled error, not a panic.
- **FR-007**: ev-db MUST provide a function to initialize a `PgPool` from a connection string, returning a clear error for missing or invalid connection parameters.
- **FR-008**: ev-db MUST provide a generic `Paginated<T>` struct with fields: `data: Vec<T>`, `total: u64`, `page: u32`, `page_size: u32`, `total_pages: u32`.
- **FR-009**: The `Paginated<T>` struct MUST correctly compute `total_pages` as `ceil(total / page_size)`.
- **FR-010**: The `Paginated<T>` struct MUST handle zero total items gracefully (`total_pages: 0`).
- **FR-011**: All public API items (functions, structs, enums) MUST have documentation comments.

### Key Entities

- **ev-core**: Shared library crate. Contains NanoID generation (public ID generation function) and canonical enum types (ConnectorType, ChargerStatus, PartnerType, StationStatus) used by all Rust services.
- **ev-db**: Shared library crate. Contains PgPool initialization from connection string (public init function) and the generic Paginated<T> struct (public struct + constructor) used by all Rust services.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: `cargo build --all` completes with zero errors and zero warnings on a clean checkout.
- **SC-002**: `cargo test --all` passes all unit tests in both crates.
- **SC-003**: 1000 NanoIDs generated in sequence with identical prefix and length have zero collisions.
- **SC-004**: Each enum type round-trips (serialize → deserialize → match original) without data loss or error.
- **SC-005**: Paginated<T> computes correct total_pages for all boundary cases (zero items, exact multiple, remainder).
- **SC-006**: Enum deserialization of an invalid string (e.g., "InvalidConnectorType") returns a controlled error rather than a panic.
- **SC-007**: A developer new to the project can run `cargo build --all` and `cargo test --all` with no additional configuration beyond Rust toolchain installation.

## Assumptions

- Rust toolchain (rustc, cargo) is installed and meets the minimum version required by any crate dependencies.
- The workspace lives at the repository root (`source/`) alongside the existing JavaScript apps — it does not create a separate top-level directory.
- No PostgreSQL database needs to be running for ev-db unit tests — pool initialization can be tested without a live connection (connection string parsing and validation are unit-testable; integration tests requiring a live DB are deferred to Sprint 2.2).
- Enum values match the data model established in MVP-1 (Sprint 1.1) — no new enum variants are introduced in this sprint.
- NanoID uses the URL-safe alphabet (A-Za-z0-9) — no special characters.
- The shared enums derive standard Rust traits (Debug, Clone, PartialEq, Serialize, Deserialize).
