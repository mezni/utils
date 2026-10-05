# Research: Project Foundation (Phase 0)

**Feature**: `001-project-foundation` | **Date**: 2026-09-26 | **Plan**: [plan.md](plan.md)

Every decision below was reached by inspecting the local toolchain or the project's own
published documents, not by assumption. Where a claim is empirical it is marked
**verified** with the command that produced it. All `NEEDS CLARIFICATION` markers from
the plan's Technical Context are resolved here.

---

## D1 — Build backend and source layout

**Decision**: `uv init --package` skeleton — `src/` layout, `uv_build` as the build
backend, `src/ecommerce/__init__.py` as the single importable package.

**Rationale**: **verified** — `uv init --package probe --no-workspace` on uv 0.12.15
generated `[build-system] requires = ["uv_build>=0.12.15,<0.13.0"]`,
`build-backend = "uv_build"`, and the tree `src/probe/__init__.py`, `.python-version`,
`.gitignore`, `pyproject.toml`. Using the tool's own first-party backend keeps the
project on one tool (constitution: "MUST NOT introduce a second framework where this
stack can reasonably meet the need"). The `src/` layout is what makes FR-007's "no
manual path configuration" true rather than accidental — nothing is importable except
the one intended package.

**Alternatives considered**:
- *hatchling* — the near-universal default and fully supported by uv. Rejected only
  because `uv_build` is the backend the chosen tooling ships and recommends, so
  choosing hatchling would mean maintaining a build dependency the project otherwise
  does not need.
- *setuptools* — rejected; legacy, and it would put a second packaging toolchain in
  the dependency graph.
- *flat layout* (`ecommerce/` at the repository root) — rejected. It makes the
  repository root itself importable, so `import tests` or `import docs` becomes
  possible, weakening FR-007 and FR-021.

## D2 — Interpreter pinning

**Decision**: `requires-python = ">=3.12"` in `pyproject.toml` **and** a committed
`.python-version` containing `3.12`.

**Rationale**: Clarification 5 requires accepting 3.12 and above while verifying only
on 3.12. `requires-python` expresses acceptance; it cannot express which version is
verified. `.python-version` does. **verified** — `uv init --package` wrote
`.python-version` with the contents `3.12` (major.minor, not a patch pin), and
`uv python find 3.12` resolved to an available CPython 3.12.13 on this machine. The
pair therefore makes FR-002 and SC-011 checkable rather than aspirational.

**Alternatives considered**:
- *`.python-version` = `3.12.13`* — rejected. Pins the patch, so a contributor cannot
  pick up a 3.12 security patch without a config change, and SC-003 (two contributors
  resolving identical versions) becomes needlessly fragile.
- *`.python-version` omitted* — rejected. Nothing would pin the verification
  interpreter, so "verified on 3.12" would be unenforceable and each contributor
  would verify whatever they happened to have.
- *A CI matrix* — rejected. A CI pipeline is explicitly out of scope in the spec's
  Assumptions; local verification on 3.12 is sufficient for this phase.

## D3 — Install command

**Decision**: `uv sync`, wrapped by `make install`.

**Rationale**: It is the completion criterion stated verbatim in `docs/plan.md` §6, it
resolves exactly what the committed lockfile pins (FR-001, FR-003), and **verified** —
`uv sync` in a probe project reported `Installed 1 package` and made the source
package importable, so FR-007's "no manual path configuration, no activation" holds.

**Alternatives considered**:
- *`pip install -e .` / `requirements.txt`* — rejected. No lockfile, so SC-003
  (identical versions across contributors) is unachievable, and it introduces a second
  dependency mechanism alongside the project's declared tooling.
- *`uv sync --frozen`* as the documented command — rejected. `--frozen` suppresses
  re-locking, which would hide the lockfile-drift edge case the spec requires to fail
  loudly. The plain form re-locks and fails on mismatch, which is the specified
  behaviour.

## D4 — Verification command

**Decision**: `make check` is the single documented verification step (FR-023). It runs,
in order: `ruff check`, `ruff format --check`, `mypy src`, `pytest`, then the two
hygiene checks (`scripts/check_repo_hygiene.py`, `scripts/check_config_template.py`).

**Rationale**: SC-001 allows at most two documented commands to reach a verified
environment; `uv sync` plus `make check` is exactly two. `docs/development.md` §31
already defines `make check` as "lint + format check + typecheck + test", so this
reuses the published target rather than inventing one. FR-019's non-zero exit
requirement is satisfied by `&&` chaining: the first failure aborts the run and
propagates its status.

**Alternatives considered**:
- *A bespoke `scripts/verify.sh` as the documented command* — rejected. It would
  duplicate `make check` and create two entry points for one job, which SC-009 forbids.
- *`make test` alone* — rejected. FR-018 requires formatting, linting, and type
  checking to be runnable, and a verification step that skips them would let a
  contributor believe the tree was checked when it was not.
- *Running the checks in parallel* — rejected. Output interleaving makes "identifies
  what needs attention" (FR-019) harder to satisfy, for no material time saving at this
  scale.

## D5 — Which dependencies are declared now

**Decision**: declare all nine libraries named in `docs/plan.md` §6 — fastapi, uvicorn,
pydantic, pydantic-settings, sqlalchemy, alembic, pytest, httpx, faker — plus three
further development tools: ruff, mypy, pytest-cov. Twelve declarations in total; pytest
is one of the nine and is classified as a development dependency.

**Rationale**: The plan names the nine explicitly as Phase 0 dependencies, and this
feature also owns Phases 1–2, which consume fastapi, uvicorn, sqlalchemy, alembic, and
httpx. Declaring them now means `uv sync` output matches the published dependency set
and later phases cause no dependency churn. The spec's Assumptions explicitly permit
declaring a not-yet-needed dependency provided its purpose is recorded, which FR-005
then makes mandatory. pytest-cov is included because `docs/development.md` §31 documents
a `make coverage` target, and FR-017 requires every documented convenience command to
be runnable one-to-one.

**Alternatives considered**:
- *Declare only what Phase 0 imports* — rejected. It contradicts `docs/plan.md` §6
  outright, and it creates dependency churn in the two phases immediately following.
- *Also add pre-commit, coverage config plugins, or a typing stub package* — rejected.
  Each lacks a demonstrated purpose (constitution, Principle V).

## D6 — Where dependency purposes are recorded

**Decision**: a *Dependency purposes* table in `docs/development.md` with columns
`Dependency | Scope | Declared for | Purpose`. The *Declared for* column is what makes
the table machine-readable by the hygiene check (D7).

**Rationale**: FR-005 requires a recorded purpose for every declaration; FR-006 requires
a recorded justification for any duplicate capability. Putting the record next to the
dependency-management workflow means the rule and its record cannot drift apart, and
`docs/development.md` is already the document a contributor reads before changing
dependencies.

**Alternatives considered**:
- *Comments in `pyproject.toml`* — rejected. They sit closer to the declaration, but
  nothing can check them, so FR-005 and SC-004 would rest on reviewer discipline alone
  for 13 rows.
- *A new `DEPENDENCIES.md`* — rejected. It would become a second source of truth
  against `docs/development.md` §7, which already owns dependency management.
- *Both the table and pyproject comments* — rejected. Two records of one fact drift.

## D7 — Reconciling the unused-dependency check with declared-but-unused dependencies

**Decision**: the unused-dependency check treats a declaration as acceptable when it is
either imported somewhere in `src/` or `tests/`, **or** listed in the D6 table with a
*Declared for* value naming a later phase of this feature. It fails otherwise.

**Rationale**: SC-006 requires that a deliberately introduced unused dependency is
caught. D5 deliberately declares libraries this phase does not import. Without this
rule the check would fire on the project's own correct state on the first run, and a
check that is red on a clean checkout is a check nobody trusts. Making the *Declared
for* column the allow-list ties FR-005, FR-006, and SC-006 to one artifact.

**Alternatives considered**:
- *Restrict the check to test-area dependencies only* — rejected. Arbitrary, and it
  leaves runtime declarations entirely unchecked.
- *Drop the unused-dependency check* — rejected. SC-006 names it explicitly.

## D8 — Enforcing the ignore rules and secret hygiene

**Decision**: `scripts/check_repo_hygiene.py`, standard library only, run by
`make check`. It fails when (a) any path matching the ignore rules is tracked, and
(b) any tracked text file contains an assignment whose key matches the sensitive set
from `docs/configuration.md` §42 (`AUTH_FAKE_TOKEN_SECRET`) or a generic
`(SECRET|TOKEN|PASSWORD|API_KEY)` pattern with a non-empty value.

**Rationale**: FR-013 asks for "automated **or** documented checks" and SC-006 requires
that a staged local configuration file is detected in 100% of injected cases. A
documented manual step cannot satisfy the 100% figure. The sensitive-key list is read
from the published reference rather than hard-coded, so it stays correct as the
reference grows.

**Alternatives considered**:
- *The pre-commit framework* — rejected. A new tool with its own config surface and its
  own failure modes, for two checks, against Principle V.
- *A shell script* — rejected. The tracked-file query and the pattern scan are both
  easier to express and to test in Python, and the project already depends on it.
- *Documented manual review only* — rejected, as above.

## D9 — Enforcing configuration-template completeness

**Decision**: `scripts/check_config_template.py`, standard library only, run by
`make check`. It extracts the variable key set from `docs/configuration.md` §42 (30
variables), extracts the key set from `.env.example`, and fails unless the two are
equal — reporting missing keys, extra keys, and entries missing a status marker.

**Rationale**: SC-007 demands 100% coverage of 30 variables with six attributes each.
Reviewing 30 rows by eye reliably misses one, which is precisely the failure the
criterion is written to prevent. Deriving the expected set from the published reference
means the check cannot itself drift out of date.

**Alternatives considered**:
- *Hand-maintained list inside the check script* — rejected. A 30-entry list duplicated
  in two places is the drift this check exists to catch.
- *Generate `.env.example` from `docs/configuration.md` at build time* — rejected. It
  adds a generation step to the install path, and the template needs hand-written
  commentary that a generator cannot produce.

## D10 — The availability-marker convention

**Decision**: one marker token, used in both places the clarifications require it.
In `.env.example`, every entry ends with `# status: honoured` or
`# status: planned (<phase>)`. In `README.md`, every command carries the same token in
its code block or list item.

**Rationale**: Clarifications 1 and 3 both resolved to "carry an explicit availability
marker", and clarifications must not introduce two vocabularies for one idea. A single
token means the D9 check and a README review test the same thing, and a reader learns
the convention once. A missing marker is non-conforming in both files (FR-009, FR-015).

**Alternatives considered**:
- *Section headings alone* — rejected. The clarifications require the marker on the
  command or setting itself; a heading does not survive a setting being added later.
- *Emoji or typographic status glyphs* — rejected. Not greppable, not screen-reader
  friendly, and not assertable by a check.

## D11 — The minimum automated check required by FR-020

**Decision**: `tests/unit/test_package.py`, asserting that `ecommerce` imports and that
`ecommerce.__version__` equals the version in `pyproject.toml`.

**Rationale**: FR-020 forbids reporting success by having collected nothing, and the
edge-case list forbids both a silent pass and an unexplained failure. A collection of
zero tests makes pytest exit non-zero, which would fail the Phase 0 completion criterion
in `docs/plan.md` §6 ("the test environment MUST execute successfully"). One real
assertion fixes the exit status and simultaneously becomes the first proof of FR-007.
Reading the version from `pyproject.toml` rather than hard-coding it keeps the assertion
true across version bumps.

**Alternatives considered**:
- *Tolerating the zero-collection exit code* — rejected. It is a pass that verified
  nothing, which FR-020 names directly.
- *A test asserting only that the tests directory exists* — rejected. It asserts nothing
  about the project and would pass even if the package were unimportable.

## D12 — Placeholder content for the three mandated test areas

**Decision**: `tests/unit/`, `tests/integration/`, and `tests/api/` each receive a
tracked `README.md` stating what belongs in that area and what it must not do.
`tests/unit/` additionally receives `test_package.py` (D11).

**Rationale**: Clarification 4 requires each area to hold at least one tracked file so
the layout survives a fresh clone, because an empty directory is invisible to version
control and FR-021 would be unverifiable. A `README.md` also answers the question a
contributor actually has — "what goes here?" — which SC-012's fresh-clone check then
confirms.

**Alternatives considered**:
- *`.gitkeep`* — rejected. It satisfies the tracking requirement but carries no purpose
  statement, so the areas exist without telling a contributor what they are for.
- *`test_placeholder.py` calling `pytest.skip`* — rejected. It pollutes collection
  counts, reads as a real test, and a permanently skipped test is a small lie about
  coverage.
- *`__init__.py` in each area* — rejected as a placeholder. It is needed only if test
  module basenames ever collide across areas, which is not true of this suite.

## D13 — Restructuring the entry-point document

**Decision**: `README.md` is split into a *Works today* section and a *Target workflow*
section. Every command carries a status token (D10). The *Target workflow* commands
whose underlying tooling does not exist yet are marked `planned`.

**Rationale**: Clarification 1 chose "both, with an explicit availability marker", and
SC-010 now measures whether a reader can identify which commands work today. The current
`README.md` presents `make bootstrap`, `alembic upgrade head`, the seed module, and a
server on port 8000 as if all were runnable; none are. This is the concrete defect the
clarification was written to fix.

**Alternatives considered**:
- *Rewrite the README to describe only what exists* — rejected by clarification 1; it
  discards the roadmap value the document already carries.
- *Leave the README as-is and rely on `docs/development.md`* — rejected. SC-010 is
  measured against the entry-point document alone.

## D14 — Version metadata, and its one duplication

**Decision**: `src/ecommerce/__init__.py` exposes `__version__ = "0.1.0"`, matching the
`APP_VERSION` default in `docs/configuration.md` §42. No automatic linkage between the
two in this phase.

**Rationale**: FR-007 needs an importable package with something to assert on, and
`APP_VERSION` participates in determinism per the published reference, so the two values
must agree. Linking them properly means reading installed distribution metadata, which
is only reliable once the package is installed as a distribution — a Phase 1 concern
when an application exists to read it.

**Alternatives considered**:
- *Read the version via `importlib.metadata` now* — rejected. It couples the domain-facing
  package to installation state, so a source checkout that was never synced would raise
  on import, which is the opposite of FR-007's intent.
- *Omit `__version__` and assert something else in the smoke test* — rejected. Version
  agreement is a real invariant worth a test; asserting `__name__` instead would be a
  weaker test dressed as the same one.

**Recorded as a known, deliberate, temporary duplication.** It is a candidate for
unification in a later phase of this feature.

## D15 — Which subpackages exist after this phase

**Decision**: only `src/ecommerce/__init__.py` is created. The `api/`, `application/`,
`domain/`, `infrastructure/`, and `seed/` subpackages documented in
`docs/architecture.md` are **not** created here.

**Rationale**: Constitution Principle V states "New abstractions MUST have a demonstrated
purpose", and the spec's Assumptions place the web application, the health endpoint,
runtime settings loading, and the migration baseline out of scope. An empty package
directory is structure without a purpose, and creating five of them would pre-empt four
later phases. Each is created by the phase that first puts code in it.

**Alternatives considered**:
- *Pre-create the full layered skeleton from `docs/plan.md` §4* — rejected on the
  Principle V grounds above, despite being the plan's published target tree. The plan
  itself states the structure "MAY evolve when feature specifications expose better
  boundaries"; this specification exposes a better boundary.
- *Create `config/` only, since the settings module is documented at
  `src/ecommerce/config/settings.py`* — rejected. Nothing in this phase writes to it;
  runtime settings loading is out of scope, so the directory would still be empty.

## D16 — Targets available after this phase

**Decision**: `Makefile` defines `help`, `install`, `test`, `test-unit`, `coverage`,
`lint`, `format`, `typecheck`, `check`, and `clean`. The remaining targets documented in
`docs/development.md` §31 — `bootstrap`, `migrate`, `migrate-new`, `migration-status`,
`seed`, `seed-test`, `seed-large`, `seed-args`, `reset`, `db-reset`, `run` — are marked
`planned` in both the Makefile's help output and the documentation.

**Rationale**: FR-017 and SC-009 make the convenience layer a one-to-one mirror of
documented plain commands. A target whose underlying command cannot run is not a
convenience, it is a broken promise. Marking them keeps the published target list
intact while being honest about availability, which is the same pattern clarifications 1
and 3 established.

**Alternatives considered**:
- *Ship all 16 targets, with the unavailable ones failing loudly* — rejected. A target
  that always fails is worse than one that is honestly marked as not yet present.
- *Trim `docs/development.md` §31 to the ten available targets* — rejected. The target
  list is a roadmap; the availability marker is the honest way to present it, per D10.

---

## Unresolved at plan time

None. Every Technical Context slot that could have carried `NEEDS CLARIFICATION` is now
answered: D1–D2 (language, packaging, interpreter), D3–D4 (dependency management,
verification), D5–D7 (dependency set and its enforcement), D8–D9 (hygiene and
configuration checks), D10–D13 (documentation conventions), D14–D16 (package contents and
command surface). The only item carried forward is the constitution divergence, which is
governance rather than a technical unknown and is recorded in the plan's Constitution
Check.
