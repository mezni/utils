---

description: "Task list for feature implementation"
---

# Tasks: FastAPI Foundation

**Input**: Design documents from `/specs/002-fastapi-foundation/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/, quickstart.md

**Tests**: Test tasks ARE included. FR-015 requires every behaviour introduced by this
feature to be covered by a check that fails when the behaviour is absent, and SC-005,
SC-006, and SC-010 all depend on the suite being real. Tests are therefore mandatory here,
not optional. Research D-09 fixes the three-layer split: `tests/unit/` for settings and
layering, `tests/integration/` for application-factory wiring, `tests/api/` for HTTP
behaviour driven in-process through `TestClient`.

**Organization**: Tasks are grouped by user story so each story can be implemented,
tested, and delivered independently.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

Single project: `src/` and `tests/` at repository root.

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Repair the Phase 0 scaffolding that blocks this feature, and lay down the
package skeleton. Everything here is a correction or a precondition recorded in plan.md
prerequisites P1–P5.

**⚠️ CRITICAL**: Phase 1 is not cosmetic. `uv sync` currently installs nothing, so no
task in any later phase can run until T004 and T005 land.

- [X] T001 Move runtime dependencies from `[dependency-group.runtime]` to `[project] dependencies` in pyproject.toml, keeping fastapi, uvicorn, pydantic, pydantic-settings, sqlalchemy, alembic, httpx, and faker (plan.md P1, research D-01)
- [X] T002 Move development dependencies from `[dependency-group.development]` to `[dependency-groups] dev` in pyproject.toml, keeping pytest, ruff, mypy, and pytest-cov (plan.md P1, research D-01)
- [X] T003 Pin the dependency-group table names to the reserved group name by confirming no other `[dependency-groups]` entries remain in pyproject.toml (plan.md P1)
- [X] T004 Re-lock dependencies with `uv lock` so `uv.lock` contains the full transitive set rather than the single `ecommerce` package, then commit the regenerated `uv.lock` (plan.md P2, research D-01)
- [X] T005 Verify the corrected `pyproject.toml` end to end with `uv sync && uv run python -c "import fastapi, pydantic_settings, pytest"` and confirm `uv run pytest --collect-only` still collects the existing tests in `tests/unit/test_package.py` (plan.md P1, P2)
- [X] T006 [P] Rename `[project.tool.ruff]` to `[tool.ruff]` and `print.line-length-limit` to `line-length = 120` in pyproject.toml (plan.md P3, research D-02)
- [X] T007 [P] Rename the `python_paths` ini option to `pythonpath` in `[project.tool.pytest.ini_options]` in pyproject.toml (plan.md P4, research D-03)
- [X] T008 Create `.env.example` documenting every setting with its type and default and no real secret, covering at least APP_NAME, APP_VERSION, APP_ENV, API_V1_PREFIX, API_DOCS_ENABLED, API_REDOC_ENABLED, HOST, and PORT (plan.md P5, FR-010, FR-012, constitution Documentation Layout)
- [X] T009 [P] Add `.env` to .gitignore and confirm no secret or local override file is tracked (FR-012)
- [X] T010 [P] Create the empty package directories `src/ecommerce/domain/` and `src/ecommerce/application/` each with an `__init__.py`, so the constitution's inward-pointing dependency rule is visible in the tree (plan.md Structure Decision)
- [X] T011 [P] Create `src/ecommerce/api/` and `src/ecommerce/observability/` each with an `__init__.py` (plan.md Structure Decision)
- [X] T012 [P] Add the `run` target to the Makefile invoking `uv run uvicorn ecommerce.main:app` bound to `HOST` and `PORT`, promoting it from the "Planned targets" list (FR-001, FR-013, Makefile)
- [X] T013 [P] Add `coverage` and `api` test targets to the Makefile if absent, so the `api` marker directory has a documented entry point (SC-008, constitution Principle IV)

**Checkpoint**: `uv sync` installs the declared dependencies, the toolchain is actually
configured, and the package tree exists. Run `make check` and confirm lint, format,
typecheck, and test all execute.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: The application factory, settings, error envelope, request identifier, and
reserved versioned router that every user story depends on. No HTTP behaviour is delivered
here — only the structure the stories plug into.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [X] T014 Implement the immutable settings object in `src/ecommerce/config.py` reading APP_NAME, APP_VERSION, APP_ENV, API_V1_PREFIX, API_DOCS_ENABLED, API_REDOC_ENABLED, HOST, and PORT with the documented defaults from contracts/configuration.md, constructed once and never mutated (FR-010, FR-011, research D-06 context)
- [X] T015 [P] Implement request-identifier resolution in `src/ecommerce/observability/request_id.py`, honouring an inbound `X-Request-ID` only when truncated and character-screened, otherwise generating a UUID4, with no logging and no metrics (FR-008, research D-06)
- [X] T016 Implement the error envelope builder and the two exception handlers in `src/ecommerce/api/errors.py`: one handler mapping `HTTPException` and `RequestValidationError` to the published code for their status, and one catch-all `Exception` handler logging the traceback server-side and returning a fixed `INTERNAL_ERROR` message, both constructing the body through the same four-field function (FR-008, FR-009, contracts/error-envelope.md, research D-08)
- [X] T017 Implement the reserved versioned router in `src/ecommerce/api/router.py` as an `APIRouter` carrying the configured `API_V1_PREFIX`, registered with no routes (FR-006, research D-07)
- [X] T018 Implement the application factory in `src/ecommerce/main.py`, exposing `create_app()`, wiring the reserved router, registering both error handlers, and setting `title` and `version` from settings so the published version is the setting rather than a literal (FR-005, FR-006)
- [X] T019 Add the `app` module-level ASGI instance in `src/ecommerce/main.py` so `uvicorn ecommerce.main:app` resolves, and confirm the default bind is loopback (FR-001, FR-013)
- [X] T020 Register the description URLs conditionally in `src/ecommerce/main.py`: `docs_url` and `openapi_url` set to `None` when `API_DOCS_ENABLED` is false, `redoc_url` set to `None` when `API_REDOC_ENABLED` is false, leaving `/health` registered independently of both (FR-004, research D-05)
- [X] T021 [P] Implement the startup failure behaviour in `src/ecommerce/config.py` so an invalid value fails startup naming the setting and its accepted values, a non-boolean switch value is an error rather than `false`, and an empty or conflicting `API_V1_PREFIX` is rejected (FR-011, contracts/configuration.md failure rules)

**Checkpoint**: The application factory builds, the reserved router is mounted under the
configured prefix, and both handlers emit the same envelope. `make check` passes. No
user story behaviour exists yet.

---

## Phase 3: User Story 1 - Start the server and see that it is alive (Priority: P1) 🎯 MVP

**Goal**: From a clean checkout, two documented commands produce a running server whose
liveness check reports the service operational and nothing more.

**Independent Test**: From a clean checkout, run `make install` then `make run`, then
request the liveness check. Complete when the server starts and the liveness check
reports the service operational. (spec.md Story 1, scenarios 1–4)

### Tests for User Story 1 ⚠️

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [X] T022 [P] [US1] Contract test asserting the liveness response is exactly `{"status": "ok"}` with no version, timestamp, uptime, or dependency field, in `tests/api/test_health_contract.py`
- [X] T023 [P] [US1] Test asserting repeated liveness requests all succeed and none touches a database, cache, or external provider, in `tests/api/test_health_availability.py`
- [X] T024 [P] [US1] Test asserting the liveness module graph imports no persistence module, in `tests/unit/test_health_independence.py`
- [X] T025 [P] [US1] Test asserting `uvicorn ecommerce.main:app` resolves to a buildable ASGI app and the default bind address is loopback, in `tests/integration/test_app_factory.py`

### Implementation for User Story 1

- [X] T026 [P] [US1] Implement the `HealthStatus` response model in `src/ecommerce/api/health.py` as a single-field schema whose `status` is the literal `ok`, with `additionalProperties` disabled so no version field can be added unnoticed (FR-002, data-model.md §1)
- [X] T027 [US1] Implement the liveness endpoint in `src/ecommerce/api/health.py` as an unversioned `GET /health` registered on the application rather than on the reserved versioned router, with no database or provider access in its call path (FR-002, FR-003, AS 2, AS 3)
- [X] T028 [US1] Extend `tests/api/test_health_availability.py` to issue repeated liveness requests, asserting every one succeeds, and confirm the suite reports a non-zero collected-test count so a run executing zero checks is treated as a failure (AS 4, SC-006)

**Checkpoint**: User Story 1 is fully functional and testable independently. From a
clean checkout, `make install && make run` then `curl /health` returns `{"status":"ok"}`.

---

## Phase 4: User Story 2 - Discover the API surface without reading source (Priority: P2)

**Goal**: The server describes itself in both human-readable and machine-readable form,
each independently switchable, and publishes a version that cannot silently drift from the
installed package.

**Independent Test**: Start the server, request the interactive description and the
machine-readable description. Complete when both are served, the machine-readable form
parses and lists the endpoints from Story 1, and disabling either description leaves the
other and the liveness check working. (spec.md Story 2, scenarios 1–5)

### Tests for User Story 2 ⚠️

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [X] T029 [P] [US2] Test asserting the machine-readable description parses without error and lists the liveness endpoint, in `tests/api/test_openapi_contract.py`
- [X] T030 [P] [US2] Test asserting the description publishes the application's title and version and that the published version equals `importlib.metadata.version("ecommerce")`, in `tests/unit/test_version_consistency.py`
- [X] T031 [P] [US2] Test asserting the description contains no `/api/v1` path entry while the reserved namespace holds no routes, in `tests/api/test_openapi_contract.py`
- [X] T032 [P] [US2] Test asserting `API_DOCS_ENABLED=false` 404s both `/openapi.json` and `/docs` while `/redoc` and the liveness check still answer, in `tests/integration/test_description_switches.py`
- [X] T033 [P] [US2] Test asserting `API_REDOC_ENABLED=false` 404s `/redoc` while `/openapi.json`, `/docs`, and the liveness check still answer, in `tests/integration/test_description_switches.py`
- [X] T034 [P] [US2] Test asserting both switches default to on when no configuration file is present, and that a non-boolean value fails startup naming the setting rather than reading as off, in `tests/unit/test_settings_defaults.py`

### Implementation for User Story 2

- [X] T035 [US2] Set the application title and version from the settings object in `src/ecommerce/main.py`, so `info.version` in the generated description is the `APP_VERSION` setting and never a literal (FR-005, AS 2)
- [X] T036 [US2] Confirm the description URL registration in `src/ecommerce/main.py` leaves the two switches genuinely independent, and add a code comment recording that they are convenience controls and not a security boundary (FR-004, research D-05)
- [X] T037 [US2] Add the description routes to `.gitignore`-adjacent documentation coverage by recording the served URLs and their controlling settings in `docs/API.md`, matching `contracts/openapi.yaml` (FR-004, constitution Documentation Layout)

**Checkpoint**: User Stories 1 and 2 both work independently. A version bump that touches
only `pyproject.toml` or only the `APP_VERSION` default fails `make check` (SC-011).

---

## Phase 5: User Story 3 - Add a real endpoint without renegotiating the foundation (Priority: P3)

**Goal**: A route mounted inside the versioned namespace appears in both descriptions and
responds at its versioned path, with no second registration step.

**Independent Test**: Mount a route inside the versioned namespace, confirm it appears in
both descriptions and responds, then remove it and confirm the foundation is unchanged.
(spec.md Story 3, scenarios 1–3)

### Tests for User Story 3 ⚠️

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [X] T038 [P] [US3] Test asserting a route added to the reserved router appears in the machine-readable description under the versioned path with no additional registration, in `tests/api/test_versioned_namespace.py`
- [X] T039 [P] [US3] Test asserting the route responds under its versioned path and not at an unversioned path, in `tests/api/test_versioned_namespace.py`
- [X] T040 [P] [US3] Test asserting an unknown unversioned path returns the four-field error envelope with `code` `NOT_FOUND`, `details` an array, and `request_id` present, in `tests/api/test_error_envelope.py`
- [X] T041 [P] [US3] Test asserting no error response body contains a stack trace, exception text, or file system path, and that a forced unhandled exception still yields a fixed `INTERNAL_ERROR` message (FR-009, SC-004) in `tests/api/test_error_envelope.py`
- [X] T042 [P] [US3] Test asserting a client-supplied `X-Request-ID` is echoed only when acceptable and is truncated and character-screened otherwise, in `tests/api/test_request_id.py`

### Implementation for User Story 3

- [X] T043 [US3] Confirm in `src/ecommerce/api/router.py` that the reserved router applies the configured prefix itself, so any route added to it inherits the versioned path with no second registration step (FR-007, AS 1, AS 2)
- [X] T044 [US3] Confirm in `src/ecommerce/api/errors.py` that route-level not-found responses pass through the same envelope builder as every other failure, rather than returning a framework default page (FR-008, AS 3)
- [X] T045 [US3] Add the versioned router to the application in `src/ecommerce/main.py` with an explicit comment that it is intentionally mounted with no routes, so a later feature does not read the empty namespace as an oversight (FR-006, research D-07)

**Checkpoint**: All three user stories are independently functional. The reserved
namespace is proven to accept a route without any change to how the server starts or is
verified.

---

## Phase 6: Polish & Cross-Cutting Concerns

- [ ] T046 [P] Amend the constitution's feature roadmap in `.specify/memory/constitution.md` to match the repository's actual directories, correcting `001-foundation` to `001-project-foundation` and shifting `002-product-catalog` and later entries by one, as a PATCH with no principle change (plan.md Divergence 1)
- [ ] T047 [P] Correct the self-contradictory `API_DOCS_ENABLED` sentence in `docs/configuration.md` §11 so it no longer claims the schema stays retrievable by a known path, matching the implemented behaviour in research D-05
- [ ] T048 [P] Update `README.md` with the project purpose, stack, and the install, start, and test commands, marking which work today, with no command left unmarked (SC-008, SC-010)
- [ ] T049 [P] Add an `Unreleased` entry to `CHANGELOG.md` recording the liveness endpoint, the two description switches, the error envelope, and the dependency-declaration correction (constitution Documentation Layout)
- [ ] T050 [P] Update `docs/development.md` with the `make install`, `make run`, and `make check` workflow and the configuration failure rules (constitution Documentation Layout)
- [ ] T051 [P] Update `docs/testing.md` to record the three test layers used here and the in-process `TestClient` approach, so the next feature extends the same structure (constitution Principle IV)
- [ ] T052 Confirm the whole verification run completes in under 60 seconds with `time make check`, and that the output shows a non-zero collected-test count (SC-007, SC-006)
- [ ] T053 Run every scenario in `quickstart.md` in the stated order and record the result, including the two-command clean-checkout path, both switch states, and the config failure cases (SC-001, SC-005, SC-010)
- [ ] T054 Confirm `uv run python -c "import sys, ecommerce.domain; print('fastapi' in sys.modules)"` prints `False`, proving the domain layer imports without the framework (constitution Principle I)
- [ ] T055 Confirm the default bind is loopback-only with `ss -ltnp | grep 8000`, and that a second startup on an occupied port fails naming the port (FR-013, FR-014)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately. **Blocks everything**,
  because `uv sync` currently installs nothing.
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories.
- **User Stories (Phases 3–5)**: All depend on Foundational completion. They can then
  proceed in parallel, or sequentially in priority order P1 → P2 → P3.
- **Polish (Phase 6)**: Depends on all desired user stories being complete.

### User Story Dependencies

- **User Story 1 (P1)**: Starts after Foundational. No dependency on other stories.
- **User Story 2 (P2)**: Starts after Foundational. Reads the factory from Foundational;
  its description must list the Story 1 endpoint, so Story 1 is the practical prerequisite
  for the full assertion, though the story is still independently testable.
- **User Story 3 (P3)**: Starts after Foundational. Depends on the reserved router from
  Foundational; independently testable because it mounts and removes its own route.

### Within Each User Story

- Tests MUST be written and MUST fail before implementation.
- Models before endpoints.
- Core implementation before integration.
- Story complete before moving to the next priority.

### Parallel Opportunities

- T006, T007, T008, T009, T010, T011, T012, T013 in Setup touch separate concerns and can
  run in parallel, but T001–T004 must complete first because re-locking depends on the
  corrected declarations.
- T015, T021 in Foundational are parallel with T014's main body; T016–T020 depend on T014
  for the settings object.
- Within each story, all test tasks are marked `[P]` and can be written in parallel.
- User Stories 1, 2, and 3 can be worked in parallel by different people once Phase 2
  completes.
- All Polish documentation tasks are marked `[P]`.

---

## Parallel Example: User Story 1

```bash
# Launch all US1 tests together:
Task: "Contract test asserting the liveness response is exactly {\"status\": \"ok\"} in tests/api/test_health_contract.py"
Task: "Test asserting repeated liveness requests all succeed in tests/api/test_health_availability.py"
Task: "Test asserting the liveness module graph imports no persistence module in tests/unit/test_health_independence.py"
Task: "Test asserting uvicorn ecommerce.main:app resolves and the default bind is loopback in tests/integration/test_app_factory.py"
```

## Parallel Example: User Story 2

```bash
# Launch all US2 tests together:
Task: "Test asserting the machine-readable description parses and lists the liveness endpoint in tests/api/test_openapi_contract.py"
Task: "Test asserting the published version equals importlib.metadata.version in tests/unit/test_version_consistency.py"
Task: "Test asserting both switches are independent in tests/integration/test_description_switches.py"
Task: "Test asserting documented defaults and non-boolean failure in tests/unit/test_settings_defaults.py"
```

## Parallel Example: User Story 3

```bash
# Launch all US3 tests together:
Task: "Test asserting a reserved-router route appears in the description in tests/api/test_versioned_namespace.py"
Task: "Test asserting the four-field envelope and no leakage in tests/api/test_error_envelope.py"
Task: "Test asserting client-supplied request ids are sanitised in tests/api/test_request_id.py"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup — without T001–T005 nothing else runs.
2. Complete Phase 2: Foundational — CRITICAL, blocks all stories.
3. Complete Phase 3: User Story 1.
4. **STOP and VALIDATE**: from a clean checkout, `make install && make run`, then
   `curl /health` returns `{"status":"ok"}` (quickstart V-1).
5. Deploy or demo the running server.

### Incremental Delivery

1. Setup + Foundational → foundation ready.
2. US1 → validate independently → the project has a running, verifiable server (MVP).
3. US2 → validate independently → the server describes itself and cannot silently drift.
4. US3 → validate independently → the namespace is proven to accept a real endpoint.
5. Each story adds value without breaking the previous ones.

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together — this is the hard serial part, and it is
   where the broken dependency declaration must be fixed.
2. Once Foundational is done:
   - Developer A: User Story 1
   - Developer B: User Story 2
   - Developer C: User Story 3
3. Stories complete and integrate independently; documentation lands in Polish.

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps each task to a user story for traceability
- Each user story is independently completable and testable
- Verify tests fail before implementing — SC-005 requires injected breakage to be caught
- Commit after each task or logical group
- Stop at any checkpoint to validate the story independently
- Avoid: vague tasks, same-file conflicts, cross-story dependencies that break independence
- Every task in a user story phase maps to a requirement in `spec.md`; the mapping is
  recorded in `checklists/requirements.md`
