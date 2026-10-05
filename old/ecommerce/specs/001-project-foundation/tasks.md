---

description: "Task list for 001-project-foundation"
---

# Tasks: Project Foundation (Phase 0)

**Input**: Design documents from `/specs/001-project-foundation/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/, quickstart.md

**Tests**: Test tasks are included because the constitution makes testing mandatory and
`quickstart.md` names the injection cases that must fail. They are written before the
implementation they cover.

**Organization**: Tasks are grouped by user story so each story can be implemented,
tested, and delivered as an independent increment.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (US1…US5)
- Every task names the exact file it creates or changes

## Path Conventions

Single `src/`-layout project at the repository root. Source in `src/ecommerce/`, tests in
`tests/{unit,integration,api}/`, project scripts in `scripts/`, design documents in `docs/`.
Layer subpackages (`api/`, `application/`, `domain/`, `infrastructure/`, `config/`,
`seed/`) are **not** created in this phase (research D15).

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Create the project skeleton, pin the interpreter, declare dependencies, and
record the resolution. Nothing here is user-visible on its own.

- [ ] T001 Create the directory skeleton `src/ecommerce/`, `scripts/`, `tests/unit/`, `tests/integration/`, `tests/api/` per `specs/001-project-foundation/plan.md` §120-148
- [ ] T002 Create `.python-version` containing `3.12` to pin the verified interpreter (FR-002, research D2)
- [ ] T003 Create `pyproject.toml` with `[build-system]` on `uv_build`, `requires-python = ">=3.12"`, project name `ecommerce`, version `0.1.0`, the `src/` layout declaration, 8 runtime dependencies (fastapi, uvicorn, pydantic, pydantic-settings, sqlalchemy, alembic, httpx, faker) and 4 development dependencies (pytest, ruff, mypy, pytest-cov) in separate dependency groups (FR-004, research D1, D5)
- [ ] T004 Run `uv lock` and commit the resulting `uv.lock`; never hand-edit it (FR-003, research D3)
- [ ] T005 [P] Create `.gitignore` covering `.env`, `data/`, `.venv/`, `__pycache__/`, `*.egg-info/`, `dist/`, `build/`, `.pytest_cache/`, `.mypy_cache/`, `.ruff_cache/`, `.coverage`, and editor/OS noise (FR-013)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: The importable package and the committed check configuration that every user
story depends on. No story work can begin until this phase is complete.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [ ] T006 [P] Create `src/ecommerce/__init__.py` exposing `__version__ = "0.1.0"` matching the `APP_VERSION` default in `docs/configuration.md` §42, importing nothing (FR-007, constitution Principle I, research D14)
- [ ] T007 Add the `[tool.ruff]` lint and format rule set, the `[tool.mypy]` configuration, and the `[tool.pytest.ini_options]` section with `testpaths = ["tests"]` to `pyproject.toml` (FR-018, FR-020, research D4)

**Checkpoint**: Foundation ready — the package imports and the check tools are configured.
User story implementation can now begin.

---

## Phase 3: User Story 1 - Install a working development environment from a clean checkout (Priority: P1) 🎯 MVP

**Goal**: A contributor on a clean checkout reaches a verified environment with the two
documented commands, `uv sync` then `make check`.

**Independent Test**: Clone into an empty directory, run `uv sync` and `make check`, and
confirm both exit 0. `make check` reaches full green once the `.env.example` (US2) and the
dependency purposes table (US3) exist, because the two hygiene checks read them; a missing
data source is reported as a failure, never as a silent pass. See *Dependencies* below.

- [ ] T008 [US1] Create `Makefile` with a `help` target listing the 10 available targets and the 11 planned targets, each carrying an availability marker (FR-017, research D16, contracts/developer-commands.md §2.2)
- [ ] T009 [US1] Add the `install` target to `Makefile` wrapping the plain command `uv sync` with no added step (FR-001, FR-017, SC-001, research D3)
- [ ] T010 [US1] Add the `check` target to `Makefile` chaining `ruff check`, `ruff format --check`, `mypy src`, `pytest`, `scripts/check_repo_hygiene.py`, and `scripts/check_config_template.py` in the order fixed by the contract, aborting at the first failure and propagating its non-zero status (FR-019, FR-023, contracts/developer-commands.md §2.1)
- [ ] T011 [US1] Add the `clean` target to `Makefile` removing `.pytest_cache/`, `.mypy_cache/`, `.ruff_cache/`, `__pycache__/`, `.coverage`, and `data/ecommerce.db` (contracts/developer-commands.md §2)
- [ ] T012 [US1] Create `scripts/check_repo_hygiene.py` using the standard library only, failing with a non-zero exit when any git-tracked path matches a rule in `.gitignore` and naming the path (FR-013, research D8)
- [ ] T013 [US1] Extend `scripts/check_repo_hygiene.py` to fail when a tracked text file assigns a non-empty value to a key listed as sensitive in `docs/configuration.md` §42 or matching the generic `(SECRET|TOKEN|PASSWORD|API_KEY)` pattern, naming the file and key (FR-014, SC-008, research D8)
- [ ] T014 [US1] Extend `scripts/check_repo_hygiene.py` to fail when a dependency declared in `pyproject.toml` is neither imported under `src/` or `tests/` nor listed with a `Declared for` value in the `docs/development.md` dependency purposes table, naming the dependency (FR-005, SC-006, research D7)
- [ ] T015 [US1] Create `scripts/check_config_template.py` using the standard library only, extracting the 30 keys from `docs/configuration.md` §42 and the keys from `.env.example`, and failing while listing both missing and extra keys when the two sets differ (FR-009, SC-007, research D9)
- [ ] T016 [US1] Extend `scripts/check_config_template.py` to fail on any `.env.example` entry with no `# status: honoured` or `# status: planned (<phase>)` token, and to fail when a default disagrees with the reference (FR-009, FR-015, research D10, contracts/config-template.md §2)
- [ ] T017 [US1] Make both scripts in `scripts/` report the offending file path, key, or dependency on stdout and exit non-zero, so a failure identifies what needs attention (FR-019, contracts/developer-commands.md §3)

**Checkpoint**: `uv sync` and `make check` are documented and runnable; the checks fail
loudly rather than silently passing.

---

## Phase 4: User Story 2 - Configure the environment locally without leaking anything (Priority: P2)

**Goal**: Every setting the project understands is discoverable in one committed,
secret-free template, each marked with whether it takes effect today; nothing local is ever
committed.

**Independent Test**: Copy `.env.example` to `.env`, confirm defaults still apply with the
file removed, confirm all 30 reference settings are present and marked, then confirm
`make check` fails for a staged `.env`.

- [ ] T018 [US2] Create `.env.example` with an entry for each of the 30 variables in `docs/configuration.md` §42 in reference order, each carrying name, default, one-line purpose, the validation rule where the value is constrained, and a `# status:` token (FR-009, SC-007, contracts/config-template.md §1-§2)
- [ ] T019 [US2] Ship the sensitive `AUTH_FAKE_TOKEN_SECRET` entry in `.env.example` commented out with a clearly synthetic placeholder value, and confirm no entry in the file holds a real credential, personal datum, or machine-specific absolute path (FR-010, SC-008, contracts/config-template.md §2.1)
- [ ] T020 [US2] Include the two reserved settings `METRICS_ENABLED` and `TRACING_ENABLED` in `.env.example` with their reserved status, and exclude the five deliberately-not-configurable items from `docs/configuration.md` §42.1 (FR-009, contracts/config-template.md §6)
- [ ] T021 [US2] Group the entries in `.env.example` under comment headings by concern — application, server, database, observability, generator, failure simulation, auth — without changing the entry format (contracts/config-template.md §2.2)
- [ ] T022 [US2] Run `make check` and confirm the configuration check passes, then delete one entry from `.env.example`, re-run, and confirm it fails naming that key before restoring the file (FR-009, FR-019, quickstart.md Scenario 6)
- [ ] T023 [US2] Remove `.env` and run `make check` to confirm the documented defaults still apply with no local configuration file present, then restore any local file (FR-012, quickstart.md Scenario 7)
- [ ] T024 [US2] Stage a local `.env` with `git add -f .env` and confirm `make check` fails naming `.env`, then unstage and delete it (FR-013, SC-008, quickstart.md Scenario 8)

**Checkpoint**: User Stories 1 and 2 both work independently; `make check` is fully green.

---

## Phase 5: User Story 3 - Add or change a dependency reproducibly (Priority: P3)

**Goal**: A dependency is added through one tooling path, classified correctly, carries a
recorded purpose, and resolves identically for every contributor.

**Independent Test**: Add one runtime and one development dependency through the documented
workflow, confirm `uv.lock` is regenerated and committed alongside, then confirm a clean
install on a second machine resolves the same versions.

- [ ] T025 [US3] Add the *Dependency purposes* table with columns `Dependency | Scope | Declared for | Purpose` to `docs/development.md`, with one row for each of the 12 declarations (FR-005, SC-004, research D6)
- [ ] T026 [US3] Record `phase 0` in the *Declared for* column for dependencies imported in this phase and `later phase (<n>)` for fastapi, uvicorn, pydantic, pydantic-settings, sqlalchemy, alembic, httpx, and faker (research D5, D7, data-model.md Dependency Declaration)
- [ ] T027 [US3] Document the add-a-dependency workflow in `docs/development.md`: declare it in the correct scope in `pyproject.toml`, record its purpose in the table, regenerate `uv.lock`, and commit the declaration and the lockfile in the same change (FR-003, FR-004, US3 scenario 1)
- [ ] T028 [US3] Document in `docs/development.md` that a dependency duplicating a capability the project already provides requires an explicit recorded justification before it is added, and that a development dependency is never required to run the application (FR-004, FR-006, US3 scenarios 3-4)
- [ ] T029 [US3] Add a dependency to `pyproject.toml` with no import anywhere and no `later phase (<n>)` record, run `make check`, confirm it fails naming that dependency, then revert the change (SC-006, research D7, quickstart.md Scenario 5)
- [ ] T030 [US3] Delete `.venv`, run `uv sync` from the clean checkout, and confirm every declared dependency resolves to the version recorded in `uv.lock` with no drift, then repeat the install and confirm an identical outcome (SC-003, quickstart.md Scenario 1)

**Checkpoint**: All three stories so far are independently functional; `make check` is green.

---

## Phase 6: User Story 4 - Run the automated checks and the test suite (Priority: P4)

**Goal**: Every documented check is runnable through a convenience target or its plain
command, reports unambiguously, and can actually fail.

**Independent Test**: Run each documented check from a clean checkout, then deliberately
introduce a violation and confirm the matching check fails with a non-zero exit.

- [ ] T031 [US4] Add the `test`, `test-unit`, and `coverage` targets to `Makefile`, wrapping `uv run pytest`, `uv run pytest tests/unit`, and `uv run pytest --cov=src/ecommerce --cov-report=term-missing` exactly (FR-017, contracts/developer-commands.md §2)
- [ ] T032 [US4] Add the `lint`, `format`, and `typecheck` targets to `Makefile`, wrapping `uv run ruff check .`, `uv run ruff format .`, and `uv run mypy src` exactly (FR-017, FR-018, contracts/developer-commands.md §2)
- [ ] T033 [US4] Mark the 11 not-yet-available targets — `bootstrap`, `migrate`, `migrate-new`, `migration-status`, `seed`, `seed-test`, `seed-large`, `seed-args`, `reset`, `db-reset`, `run` — as planned in the `Makefile` help output and in `docs/development.md` §31, so each is visible as planned and none is present-and-failing; this completes the `Makefile` started in T008 and must not run concurrently with it (research D16, contracts/developer-commands.md §2.2)
- [ ] T034 [P] [US4] Create `tests/unit/test_package.py` asserting that `ecommerce` imports and that `ecommerce.__version__` equals the version read from `pyproject.toml`, so the suite never reports success by collecting nothing (FR-020, research D11)
- [ ] T035 [P] [US4] Create `tests/unit/README.md` stating what belongs in the unit area and what it must never do, including execution-order dependence and mutation of development data (FR-021, FR-022, research D12)
- [ ] T036 [P] [US4] Create `tests/integration/README.md` stating what belongs in the integration area and that it will cover SQLite persistence and transactions once a database exists (FR-021, constitution Principle IV, research D12)
- [ ] T037 [P] [US4] Create `tests/api/README.md` stating what belongs in the API area and that it will cover HTTP status codes, request validation, and error responses once an API exists (FR-021, constitution Principle IV, research D12)
- [ ] T038 [US4] Run `git clean -xdn` and `ls tests/unit tests/integration tests/api` to confirm all three areas are present on a fresh clone with at least one tracked file each and nothing untracked required (FR-021, SC-012, quickstart.md Scenario 4)
- [ ] T039 [US4] Run each plain command in `contracts/developer-commands.md` §2 directly and confirm the result is identical to running the corresponding target in `Makefile`, with no step existing only inside the convenience layer (FR-017, SC-009, quickstart.md Scenario 11)
- [ ] T040 [US4] Inject each of the four violations named in SC-006 — formatting drift, a failing test, an unused dependency, a staged local configuration file — and confirm `make check` fails with a non-zero exit naming the file, test, or dependency, reverting each immediately (FR-019, SC-006, quickstart.md Scenario 5)
- [ ] T041 [US4] Time the full `make check` run from a clean checkout and confirm it completes in under 60 seconds with zero failures and a non-zero collected-test count (SC-005, quickstart.md Scenario 3)

**Checkpoint**: All four stories so far are independently functional.

---

## Phase 7: User Story 5 - Understand the project from its entry-point documentation (Priority: P5)

**Goal**: A reader given only `README.md` can state the purpose, the stack, each standard
workflow command, and which of those commands work today.

**Independent Test**: Give a first-time reader `README.md` alone and ask them to state the
project purpose, supported stack, and the install, run, migrate, seed, and test commands,
and to mark which work today.

- [ ] T042 [US5] Restructure `README.md` into a *Works today* section and a *Target workflow* section, replacing the current presentation of `make bootstrap`, `alembic upgrade head`, the seed module, and the port-8000 server as runnable (FR-015, research D13)
- [ ] T043 [US5] State the project purpose, the supported stack, and the install, run, migrate, seed, and test commands in `README.md` without requiring the reader to open another file (FR-015, SC-010)
- [ ] T044 [US5] Add the `# status: honoured` or `# status: planned (<phase>)` token to every command in both `README.md` sections, using the same vocabulary as `.env.example` (FR-015, research D10, contracts/config-template.md §3)
- [ ] T045 [US5] Show the single shortcut workflow command with its equivalent explicit plain commands beside it in `README.md`, so the shortcut is a convenience and never a prerequisite (FR-015, FR-017, SC-009)
- [ ] T046 [P] [US5] Add the Phase 0 foundation entry to `CHANGELOG.md` in the same change as the behaviour it records (FR-016, US5 scenario 3)
- [ ] T047 [US5] Read `README.md` alone and confirm a first-time reader can state the purpose, the stack, each workflow command, and which of them work today, with no unmarked command in either section (SC-010, quickstart.md Scenario 10)

**Checkpoint**: All five user stories are independently functional.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Changes that affect more than one user story.

The first four tasks close coverage on requirements that no single story phase can assert,
because each is an end-state audit that is only meaningful against the fully assembled
repository.

- [ ] T048 [P] Add the coverage target and the available/planned target status to the command list in `docs/development.md` §31 (plan.md §132, research D16)
- [ ] T049 [P] Document the three-area test layout and what each area covers in `docs/testing.md` (constitution Project Documentation Layout, FR-021)
- [ ] T050 [P] Run `git check-ignore -v .env data/ .venv/` and confirm each ignored path is attributed to a rule in `.gitignore`, and confirm `git status` stages none of them (FR-013, quickstart.md Scenario 8)
- [ ] T051 Record the deferred `001-foundation` feature-slug divergence and its planned resolution in `CHANGELOG.md` so the constitution conflict stays visible rather than silent; runs after T046 because both append to the same file (constitution Governance, plan.md §89-94)
- [ ] T052 [P] From a fresh clone with no `.venv`, run the single documented install command and confirm it succeeds on the first attempt with no manual intervention and no undocumented configuration (SC-002, FR-001)
- [ ] T053 [P] Attempt `uv sync` under a Python 3.11 interpreter and confirm it fails with a message naming 3.12 as the required version rather than a downstream or unrelated error, then confirm `uv sync` and `make check` both pass under 3.12; the behaviour under test is `requires-python` in `pyproject.toml` and the pin in `.python-version` (SC-011, FR-002)
- [ ] T054 [P] Assert that no file under `src/` hard-codes a configuration value and that `scripts/check_config_template.py` is the only reader of `.env.example`, recording that a settings module is deliberately absent this phase (FR-011, contracts/config-template.md §5)
- [ ] T055 Assert that no declared dependency or created module introduces a service, message broker, distributed runtime, event sourcing, or CQRS, and record the result in `CHANGELOG.md` with the Feature Phase 0 entry; runs after T046 and T051 because all three append to the same file (FR-008)
- [ ] T056 Run all eleven scenarios in `quickstart.md` end to end from a clean checkout and record the result of each; a green run of all eleven is the definition of done for this feature (quickstart.md)
- [ ] T057 [P] Re-read the constitution gate table in `plan.md` §175-186 and confirm each recorded verdict still holds against the delivered tree, reporting any gate that no longer passes (plan.md §175-186, constitution Governance)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately.
- **Foundational (Phase 2)**: Depends on Phase 1 — BLOCKS all user stories.
- **User Stories (Phase 3–7)**: All depend on Phase 2 completion.
- **Polish (Phase 8)**: Depends on all five user stories being complete.

### User Story Dependencies

- **US1 (P1)**: Starts after Phase 2. No dependency on another story's *implementation*, but
  its `make check` target reads artifacts produced by US2 (`.env.example`) and US3 (the
  dependency purposes table). Both checks report a missing data source as a failure rather
  than a silent pass, so `make check` reaches full green only after US2 and US3 land. This is
  the one place where the spec's story independence and the command contract interact, and it
  is resolved by ordering, not by weakening either.
- **US2 (P2)**: Starts after Phase 2. Depends on T015/T016 (the config check) to be green.
- **US3 (P3)**: Starts after Phase 2. Depends on T014 (the unused-dependency check) to be green.
- **US4 (P4)**: Starts after Phase 2. Extends `Makefile`, so it must not run concurrently with
  US1's `Makefile` tasks (T008–T011).
- **US5 (P5)**: Starts after Phase 2. Independent of the others; no shared file except
  `CHANGELOG.md` with T051.

### Within Each User Story

- Test and verification tasks run after the artifacts they check exist.
- `Makefile` tasks are sequential — every one edits the same file.
- Scripts before the checks that invoke them; check scripts before the `make check` target
  that runs them.
- A story is complete when its checkpoint passes, not when its last task is checked off.

### Parallel Opportunities

- T005, T006, T034–T037, T046, T048–T050, T052–T054, T057 are marked `[P]`.
- T033, T051, T055, T056 are not marked `[P]`: T033 extends the `Makefile` that T008
  creates, and T051 and T055 both append to the `CHANGELOG.md` that T046 writes.
- Phases 1 and 2 contain no parallel work by design — each task edits a file the next one
  depends on.
- Once Phase 2 completes, US2, US3, and US5 can be worked in parallel by different people.
  US1 and US4 both touch `Makefile` and must be serialised against each other.

---

## Parallel Example: User Story 2

```bash
# T018 first — it creates the file the rest refine.
Task: "T018 [US2] Create .env.example with an entry for each of the 30 variables in docs/configuration.md §42"

# Then, sequentially (same file):
Task: "T019 [US2] Ship the sensitive AUTH_FAKE_TOKEN_SECRET entry commented out"
Task: "T020 [US2] Include the two reserved settings METRICS_ENABLED and TRACING_ENABLED"
Task: "T021 [US2] Group the entries in .env.example under comment headings by concern"

# Verification tasks run last and are independent of each other.
Task: "T022 [US2] Run make check and confirm the configuration check passes"
Task: "T023 [US2] Remove .env and run make check to confirm defaults still apply"
```

---

## Parallel Example: User Story 4

```bash
# Two Makefile task groups, run sequentially against each other.
Task: "T031 [US4] Add the test, test-unit, and coverage targets to Makefile"
Task: "T032 [US4] Add the lint, format, and typecheck targets to Makefile"

# These five touch different files and run together.
Task: "T033 [P] [US4] Mark the 11 not-yet-available targets as planned"
Task: "T034 [P] [US4] Create tests/unit/test_package.py"
Task: "T035 [P] [US4] Create tests/unit/README.md"
Task: "T036 [P] [US4] Create tests/integration/README.md"
Task: "T037 [P] [US4] Create tests/api/README.md"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup.
2. Complete Phase 2: Foundational — CRITICAL, blocks all stories.
3. Complete Phase 3: User Story 1 — `uv sync` and `make check` exist and fail loudly.
4. Complete Phases 4 and 5 (US2, US3) so the two hygiene checks have the artifacts they read.
5. **STOP and VALIDATE**: run `quickstart.md` Scenarios 1–3. `make check` is green and the
   two-command path is proven.
6. Deploy/demo if ready.

### Incremental Delivery

1. Setup + Foundational → foundation ready.
2. US1 + US2 + US3 → `uv sync` and `make check` both exit 0 — the two-command path (MVP).
3. US4 → the full check surface and a non-empty test suite.
4. US5 → the entry-point document tells the truth about all of the above.
5. Polish → cross-cutting documentation and the full quickstart run.
6. Each story adds value without breaking the previous ones.

### Parallel Team Strategy

1. The team completes Setup and Foundational together.
2. Once Phase 2 is done:
   - Developer A: US1 (`Makefile` install/check/clean, both check scripts).
   - Developer B: US2 (`.env.example`).
   - Developer C: US3 (`docs/development.md` dependency table).
   - Developer D: US4 (`Makefile` remaining targets, the three test areas) — starts after A.
   - Developer E: US5 (`README.md`, `CHANGELOG.md`).
3. Stories integrate independently; `make check` goes fully green once A, B, and C finish.

---

## Notes

- `[P]` marks tasks that touch different files and have no unfinished dependency.
- `[Story]` labels map every task to the user story it serves, for traceability.
- Every task names the file it creates or changes, so no task needs further context.
- A check that cannot fail is a defect: every verification task asserts a non-zero exit,
  not just a zero one.
- Commit after each task or logical group; revert immediately after each injected violation.
- Stop at any checkpoint to validate the story independently before moving on.
- Avoid: same-file concurrency (`Makefile`, `pyproject.toml`, `.env.example`), and any task
  that would make a check pass by collecting or asserting nothing.
