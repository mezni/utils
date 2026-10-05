# Feature Specification: FastAPI Foundation

**Feature Branch**: `002-fastapi-foundation` (not created — no branch hook registered)

**Created**: 2026-09-26

**Status**: Draft

**Input**: User description: "read from docs/plan.md phase 1" — Phase 1, *FastAPI
Foundation*, `docs/plan.md` §7

## Clarifications

### Session 2026-09-26

- Q: When the server publishes its own version in the machine-readable API description, which value is authoritative — the installed package's version or the `APP_VERSION` setting? → A: The setting supplies the published value and a test asserts the package version equals it.
- Q: Should this phase implement the two documented settings that switch the API descriptions on and off, or serve the descriptions unconditionally and leave the switches for later? → A: Honour both switches now, with the documented defaults, so the descriptions can be turned off individually.
- Q: How should the reserved versioned namespace be made visible in the machine-readable API description while it still contains no endpoints? → A: Drop the requirement.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Start the server and see that it is alive (Priority: P1) 🎯 MVP

A developer who has just cloned the repository runs the two documented commands and
gets a running server that answers a liveness check. They learn that the foundation
is real by observing a response, not by reading source.

**Why this priority**: Every later phase of this project attaches to a running HTTP
server. Without a server that starts and reports itself healthy, no product, customer,
cart, order, inventory, or payment behaviour can be exercised at all. This story alone
is a viable, demonstrable increment.

**Independent Test**: From a clean checkout, run the documented install command and the
documented start command, then request the liveness check. The increment is complete
when the server starts and the liveness check reports the service operational. It
delivers value on its own: a working, verifiable HTTP endpoint.

**Acceptance Scenarios**:

1. **Given** a clean checkout with no local configuration file, **When** the
   contributor runs the documented install command, **Then** the project installs
   successfully and the server can subsequently be started.
2. **Given** an installed project, **When** the contributor runs the documented start
   command, **Then** a server begins listening and reports no startup error.
3. **Given** a running server, **When** the liveness check is requested, **Then** the
   response indicates the service is operational, and carries no dependency-derived or
   version detail.
4. **Given** a running server, **When** the liveness check is requested repeatedly,
   **Then** every request succeeds — the check carries no dependency that could make
   it fail for an unrelated reason.

---

### User Story 2 - Discover the API surface without reading source (Priority: P2)

A developer or an integrating client needs to know what the server exposes and how to
call it. They obtain that from the server itself, in both human-readable and
machine-readable form, instead of reading implementation files.

**Why this priority**: The project exists to be consumed by other systems and by
test suites. Self-description removes guesswork, and it is the mechanism by which
routes become discoverable without reading source. It depends on Story 1 only in that a
server must be running.

**Independent Test**: Start the server, then request the interactive description and the
machine-readable description. The increment is complete when both are served and the
machine-readable form is valid, parseable, and lists the endpoints from Story 1.

**Acceptance Scenarios**:

1. **Given** a running server with descriptions enabled, **When** the interactive
   description is requested in a browser, **Then** a navigable, human-readable page
   listing the available endpoints is shown.
2. **Given** a running server with descriptions enabled, **When** the machine-readable
   description is requested, **Then** a valid, parseable description of the API is
   returned, naming the application's title, version, and available endpoints.
3. **Given** the machine-readable description, **When** it is parsed by a client tool,
   **Then** parsing succeeds without error and every endpoint from Story 1 appears in
   it.
4. **Given** a running server with a route mounted inside the versioned namespace,
   **When** a consumer inspects the machine-readable description, **Then** that route
   appears under the versioned namespace, so a consumer can see where future endpoints
   will appear.
5. **Given** a running server with one description switched off, **When** the disabled
   description is requested, **Then** it is not served, while the other description and
   the liveness check continue to work.

---

### User Story 3 - Add a real endpoint without renegotiating the foundation (Priority: P3)

A developer adding the first genuine business capability later in the project must be
able to place it inside the versioned namespace and have it appear in both descriptions
automatically, without altering how the server is started or verified.

**Why this priority**: This is the forward-compatibility guarantee the foundation
exists to provide. It is the least urgent because it is proven by inspection until the
first real endpoint arrives, but it is the requirement that keeps later features from
each inventing their own layout.

**Independent Test**: Mount a placeholder route inside the versioned namespace, restart
the server, and confirm it appears in both descriptions and responds. Remove the
placeholder and confirm the foundation is unchanged.

**Acceptance Scenarios**:

1. **Given** a route mounted inside the versioned namespace, **When** the server starts,
   **Then** that route appears in both the interactive and machine-readable descriptions
   without additional registration steps.
2. **Given** a route inside the versioned namespace, **When** it is requested, **Then** it
   responds under the versioned path rather than at an unversioned path.
3. **Given** the versioned namespace is established, **When** a consumer requests an
   unversioned path that does not exist, **Then** the server reports it as not found
   using the project's standard error structure, rather than returning an unstructured
   failure.

---

### Edge Cases

- **No local configuration file present.** Every setting has a documented default, so a
  contributor who never creates a local configuration file still gets a working server.
  Deleting the local configuration file MUST NOT break startup.
- **Local configuration file is partially filled or empty.** A contributor truncating
  their configuration file gets the documented defaults for everything absent, not a
  startup failure.
- **A port is already in use.** Startup failure MUST name the port and the remedy
  rather than surfacing a low-level error the contributor cannot act on.
- **Commands run from the wrong directory.** Startup from a directory that is not the
  project root MUST fail with a message naming the problem, not with an unrelated import
  or file-not-found error.
- **An unknown path is requested.** Not-found MUST use the project's standard error
  structure, and MUST NOT leak an internal exception, a stack trace, or a file system
  path.
- **The server is started twice in quick succession.** The second start MUST fail
  cleanly on the occupied port rather than corrupting state or hanging.
- **The liveness check is called while a later phase's subsystem is degraded.** The
  liveness check MUST stay independent of storage, caches, and external providers, so it
  keeps reporting process health when a dependency is down. Distinguishing liveness from
  readiness is deferred, per the constitution.
- **A configuration value is invalid.** The failure MUST name the setting and the
  accepted values, and MUST NOT silently fall back to a default that masks the mistake.
- **A description switch is set to a non-boolean value.** The failure MUST name the
  setting and MUST NOT be interpreted as "off", because a typo that silently disables
  the API descriptions would look like a working server with no documentation.
- **The versioned namespace prefix is set to an empty or conflicting value.** Startup
  MUST fail with a message naming the setting, because an empty prefix would silently
  place future business routes outside the versioned namespace and break the contract
  every client depends on.
- **Reloading during development.** A reload that fails MUST leave the previous working
  process available rather than taking the developer down with it.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The system MUST provide a single documented command that starts the
  application server, and a developer MUST be able to reach a running server from a
  clean checkout in at most two documented commands.
- **FR-002**: The system MUST expose a liveness check that reports whether the running
  process is operational, and MUST NOT include the application version in that response —
  the version belongs to the readiness check, which is deferred.
- **FR-003**: The liveness check MUST NOT depend on a database, cache, migration state,
  or any external provider, so that it reports process health independently of every
  subsystem.
- **FR-004**: The system MUST serve a human-readable, interactive description of its
  endpoints, and a machine-readable description, both generated from the server itself
  rather than maintained by hand. Each description MUST be independently switchable
  through a documented setting, so that turning one off does not disable the other, and
  the settings MUST take their documented default values when no local configuration
  file is present. Turning the descriptions off MUST NOT disable the liveness check. The
  switches MUST NOT be treated as a security control: they exist to reduce accidental
  exposure in a given environment, and no sensitive behaviour may depend on them.
- **FR-005**: The application MUST declare its own identity — title, version, and
  description — and that identity MUST appear in the machine-readable description. The
  published version MUST be supplied by the application's version setting rather than a
  separately maintained literal, and an automated check MUST assert that the installed
  package's version equals that setting, so the two cannot drift apart unnoticed.
- **FR-006**: The system MUST reserve a single versioned route namespace for all public
  routes. The namespace prefix MUST be supplied by configuration rather than hard-coded,
  and MUST take the project's documented first-version value as its default when no local
  configuration file is present.   Operational endpoints, including the liveness check,
  MUST sit outside this namespace so that a probe need not know the API version. The
  namespace is not required to appear in the machine-readable description while it holds
  no routes; it becomes visible when the first route is mounted inside it.
- **FR-007**: Routes mounted inside the versioned namespace MUST appear in both
  descriptions and MUST be reachable at their versioned paths, with no additional
  registration step beyond declaring the route.
- **FR-008**: The system MUST return a not-found response for an unknown path using one
  consistent error structure carrying a stable machine-readable code, a human-readable
  message, an array of field-level details, and a request identifier.
- **FR-009**: The system MUST NOT expose an internal exception, stack trace, or file
  system path in any error response.
- **FR-010**: The application MUST start with no local configuration file present, using
  documented defaults, and MUST continue to start when that file is absent, empty, or
  partially filled.
- **FR-011**: The application MUST read its configuration from the environment rather
  than from values hard-coded in source, and an invalid value MUST fail startup with a
  message naming the setting and its accepted values rather than falling back silently.
- **FR-012**: The system MUST NOT require a local configuration file to contain any
  secret for the foundation to run, and no secret MUST be committed to the repository.
- **FR-013**: The application MUST bind to a loopback address by default when started
  for development, so a default start is not reachable from other machines.
- **FR-014**: Startup failure MUST report a cause a contributor can act on — naming the
  occupied port, the wrong working directory, or the invalid setting — rather than
  surfacing an unrelated low-level error.
- **FR-015**: Every behaviour introduced by this feature MUST be covered by at least one
  automated check that fails when the behaviour is absent, so the foundation cannot
  regress to a server that starts but does not answer.
- **FR-016**: The system MUST be structured so that the delivery layer depends inward on
  application and domain concerns, and the domain layer MUST remain free of framework
  dependencies, so the domain stays unit-testable without a server, database, or event
  loop.
- **FR-017**: The system MUST remain a single deployable unit; this feature MUST NOT
  introduce a service boundary, message broker, background worker, event store, or
  command/query separation.
- **FR-018**: This feature MUST remain limited to the application entry point, its
  identity, its liveness check, its versioned namespace, its API description switches,
  and its error structure. It MUST NOT add business capabilities, persistence,
  migrations, generated data, failure simulation, authentication, cross-origin access,
  or request instrumentation.

### Key Entities

- **Application identity**: the self-declared name, version, and description the server
  reports about itself, visible in the machine-readable description. The version is
  supplied by the application's version setting; the installed package's version is
  required to equal it, and the requirement is asserted rather than assumed.
- **Liveness report**: the observation returned by the liveness check — that the process
  is operational. Deliberately carries neither a version nor any dependency status,
  because a liveness probe that fails when a dependency is slow would restart a healthy
  process, and the version belongs to the deferred readiness check.
- **Route**: one reachable endpoint, its path, the versioned namespace it sits in, and
  the description entry it generates.
- **Versioned namespace**: the single reserved prefix under which all public routes will
  live. Its value comes from configuration with a documented first-version default.
  Exists before any business route is mounted, and sits apart from the
  operational endpoints so a probe need not know the API version.
- **Description switch**: one boolean per human-readable description, deciding whether
  that description is served. Two independent switches, each defaulting to on outside
  production. Convenience controls only — a disabled description is not a security
  boundary.
- **Error report**: the single structure returned for every failure — stable code,
  human-readable message, array of field-level details, and request identifier.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 100% of contributors starting from a clean checkout reach a running,
  liveness-reporting server using the two documented commands and no manual
  configuration, with no undocumented step in between.
- **SC-002**: The liveness check returns a success response in 100% of repeated
  requests, and remains unaffected by the presence or absence of any other subsystem.
- **SC-003**: Both descriptions of the API surface are served in 100% of runs when
  enabled, the machine-readable form parses successfully without error in 100% of
  automated checks, and disabling either one leaves the other and the liveness check
  working in 100% of those runs.
- **SC-004**: 100% of unknown paths return the standard error structure; 0 responses
  expose an internal exception, a stack trace, or a file system path.
- **SC-005**: A deliberately broken invariant — server not starting, liveness check
  absent, description unparseable, or error structure wrong — is detected by the
  documented verification command in 100% of injected cases, with a non-zero exit and a
  message identifying what failed.
- **SC-006**: 100% of automated checks covering this feature execute and pass; a run
  reporting success while executing zero checks is treated as a failure.
- **SC-007**: The full documented verification run completes in under 60 seconds on a
  developer workstation.
- **SC-008**: A contributor reading only the entry-point document can state the
  project's purpose, its stack, and the install, start, and test commands, and can mark
  which of them work today — with no command left unmarked.
- **SC-009**: A contributor whose configuration file is deleted, empty, or partially
  filled still reaches a running server in 100% of those cases.
- **SC-010**: Every command added to the entry-point document by this feature is
  runnable on its own, and running the convenience form produces the same result as
  running the plain form, for 100% of commands.
- **SC-011**: The version published in the machine-readable description and the installed
  package's version are equal in 100% of verification runs, so a version bump that
  updates only one of them fails the documented verification command.

## Assumptions

- The technology baseline is inherited as a project constraint rather than chosen by
  this feature: the interpreter floor, web framework, validation library, async server,
  and dependency manager are already fixed by the project constitution and
  `docs/plan.md` §2.
- The dependency resolution recorded during Phase 0 is available and unchanged by this
  feature; no new dependency is introduced.
- The project skeleton, its importable package, and its verification command exist
  before this feature begins, because this feature extends them rather than creating
  them.
- No persistence exists at this point. The liveness check is therefore specified to be
  independent of a database, and no storage behaviour is assumed.
- **Authentication is out of scope for this feature.** `docs/plan.md` §18 places
  authentication in Phase 12, and the constitution requires authentication and
  authorization boundaries to remain separate from business logic. The liveness check is
  specified as unauthenticated so that it stays usable as a monitoring probe. During
  clarification, OAuth2 was selected as the project's authentication approach; that
  decision is recorded here and carried forward to the authentication feature, and it
  constrains nothing in this feature.
- **Data retention is out of scope for this feature.** Nothing in `docs/plan.md` §7
  creates persistent user data, so no retention period is specified. A retention policy
  will be specified by the feature that first creates data with a lifecycle.
- A readiness endpoint MAY be added by a later feature; this feature specifies only a
  liveness check, per the constitution.
- Structured request logging, request identifiers, and metrics belong to the
  observability phase. This feature specifies the request identifier field in the error
  structure because the error contract mandates it, and nothing more.
- Failure simulation, generated fake data, and business capabilities are out of scope;
  each is a separate later phase.
- The API contract document is the authority for the full error-code list and status-code
  set. This feature requires only that the error structure and the not-found case are
  honoured from the first version onward.
- The published configuration reference is the authority for setting names and their
  defaults, including the versioned prefix and the two description switches. This feature
  reads those settings; it does not add settings of its own.
- The configuration template that the Phase 0 specification committed to is not yet
  present in the repository. This feature's configuration requirements depend on it, so
  the dependency is recorded rather than treated as satisfied.
- Cross-origin access is out of scope for this phase: there is no browser client to serve
  yet, and the published default is off with explicit origins required. It is named here
  so that its absence is deliberate rather than an oversight.
