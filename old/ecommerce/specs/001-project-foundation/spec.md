# Feature Specification: Project Foundation (Phase 0)

**Feature Branch**: `001-project-foundation` (not created — no branch hook registered)

**Created**: 2026-09-26

**Status**: Draft

**Input**: User description: "read from docs/plan phase 0" — Phase 0 *Project Foundation* as defined in `docs/plan.md` §6, read under the governance of `.specify/memory/constitution.md` v1.1.1

## Clarifications

### Session 2026-09-26

- Q: Must the entry-point document describe only the commands that work today, or may it also document the full intended workflow that does not exist yet? → A: Both — a "works today" section plus a "target workflow" section, where every command carries an explicit availability marker.
- Q: Does the constitution's feature list need to be amended now, or can it be read as an illustrative roadmap that the actual `specs/` layout no longer has to match? → A: Treat the constitution's list as illustrative and record it as a deferred conflict to be resolved when the second feature is specified.
- Q: Does the configuration template have to cover every setting in the published configuration reference, or only the settings this phase actually honours? → A: Every setting in the published reference, each marked with whether it is honoured yet.
- Q: Must the three test areas exist as real, version-controlled locations from this phase, even though only unit-level checks are meaningful before any application code exists? → A: Each of the three areas holds a tracked placeholder file, so the layout survives a fresh clone and FR-021 is verifiable.
- Q: Which interpreter versions must the foundation actually be verified on, given that "3.12 or later" already permits anything newer? → A: Accept any version from 3.12 upward, but verify the foundation only on 3.12 — the documented minimum.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Install a working development environment from a clean checkout (Priority: P1)

A developer who has just received repository access clones the project and follows the documented install command once. They end up with an environment that can import the project's source package, and a verification step that tells them their environment matches the documented baseline — without editing any file, choosing an interpreter by hand, or resolving a dependency conflict themselves.

**Why this priority**: Nothing can be built, run, or tested until a contributor can install the project. Every later feature depends on this story already being true, so it blocks all other work.

**Independent Test**: Clone into an empty directory on a machine with no project tooling installed, run the documented install command and then the documented verification command, and confirm both report success. Delivers value on its own: a contributor can now write and execute code against the project.

**Acceptance Scenarios**:

1. **Given** a clean checkout on a supported interpreter, **When** the contributor runs the single documented install command, **Then** it completes successfully, resolves exactly the versions recorded by the project, and requires no manual intervention.
2. **Given** a clean checkout, **When** the contributor runs the documented verification command, **Then** it reports success and confirms the interpreter version, the recorded dependency versions, and the importable source package are all as expected.
3. **Given** a machine whose interpreter is older than the supported baseline, **When** the contributor attempts to install, **Then** it fails with a message naming the required version instead of failing later with an obscure error.
4. **Given** an installed environment, **When** the contributor imports the project source package from any working directory, **Then** the import succeeds without manual path configuration and without activating the environment first.

---

### User Story 2 - Configure the environment locally without leaking anything (Priority: P2)

A contributor copies the shipped configuration template to a local configuration file, changes only what their machine needs, and leaves everything else at documented defaults. They can see every setting the project understands — its type, its default, and what it does — from that one file, and their personal values never reach the shared repository.

**Why this priority**: Configuration that has to be discovered by reading source code slows every onboarding, and configuration that is easy to commit by accident creates security incidents. It also unblocks every later feature, all of which read configuration.

**Independent Test**: Copy the template, confirm defaults apply when no local file exists, and confirm every documented setting is represented. Then check the repository history for committed local configuration or credentials. Delivers value on its own: every later feature becomes configurable.

**Acceptance Scenarios**:

1. **Given** a clean checkout with no local configuration file, **When** the verification step or the project is started, **Then** the documented defaults apply and no error occurs.
2. **Given** the shipped configuration template, **When** a contributor inspects it, **Then** every setting in the published configuration reference is listed with its name, type, default value, purpose, the rule applied to constrained values, and whether it is honoured in the current phase.
3. **Given** a contributor has edited a local configuration file, **When** the edited file is staged for commit, **Then** the repository's ignore rules and documented checks prevent it from being committed.
4. **Given** the shipped configuration template, **When** it is reviewed, **Then** it contains no real secret, credential, personal data, or machine-specific absolute path.

---

### User Story 3 - Add or change a dependency reproducibly (Priority: P3)

A contributor needs a capability the project does not yet have. They declare it through the project's own dependency tooling, classify it correctly as runtime or development-only, state why it is needed, and the change stays reproducible for everyone else. No second, competing tool is introduced to manage dependencies.

**Why this priority**: Dependency drift is the most common source of "works on my machine" failures, and a dependency adopted without a stated purpose is how a deliberately simple project accumulates accidental complexity. This has to be settled before the first feature lands, not after.

**Independent Test**: Add one runtime dependency and one development-only dependency through the documented workflow, then confirm a second contributor's clean install produces the same versions and that each addition has a recorded purpose. Delivers value on its own: the project can grow without losing reproducibility.

**Acceptance Scenarios**:

1. **Given** a contributor needs a new capability, **When** they declare it through the project's dependency tooling and record its purpose, **Then** the recorded resolution is regenerated and committed as part of the same change.
2. **Given** a change to the declared dependencies, **When** a second contributor installs from a clean checkout, **Then** they resolve the same versions as the first contributor.
3. **Given** a dependency needed only for development-time checks, **When** it is declared, **Then** it is classified as a development dependency and is not required in order to run the application.
4. **Given** a proposed dependency that duplicates a capability the project already has, **When** it is proposed, **Then** adding it requires an explicit, recorded justification.

---

### User Story 4 - Run the automated checks and the test suite (Priority: P4)

Before opening a change, a contributor runs the project's automated checks — formatting, linting, type checking where adopted, and the test suite — through either the convenience commands or the plain commands those convenience commands wrap. They get an unambiguous pass or fail, including when something is wrong.

**Why this priority**: The constitution makes testing mandatory, and the whole purpose of the project is to be a trustworthy target for other people's tests. A verification step that cannot fail is worse than no verification step.

**Independent Test**: From a clean checkout, run each documented check. Then deliberately introduce a violation — formatting drift, a failing test, an unused dependency — and confirm each check reports failure with a non-zero exit status. Delivers value on its own: changes can be trusted before review.

**Acceptance Scenarios**:

1. **Given** a clean checkout, **When** the contributor runs the full check command, **Then** every constituent check runs and the overall result is reported unambiguously.
2. **Given** a check that fails, **When** it runs, **Then** it reports a non-zero exit status and identifies what needs attention.
3. **Given** a clean checkout in which no application code exists yet, **When** the test suite is executed, **Then** it executes and reports success rather than aborting because it found nothing to run.
4. **Given** the tests directory on a fresh clone, **When** a contributor inspects its layout, **Then** the three mandated test areas — unit, integration, and API — are all present as distinct, version-controlled locations, each holding at least one tracked file.
5. **Given** a contributor who prefers plain commands, **When** they run the command underlying any documented convenience command directly, **Then** the result is identical; the convenience layer adds no hidden steps and is never a prerequisite.

---

### User Story 5 - Understand the project from its entry-point documentation (Priority: P5)

A new contributor reads the repository's entry-point document and learns what the project is, what it is for, what it is built with, and the exact commands for the standard workflow. They do not need to open the internal design documents to get started.

**Why this priority**: Documentation is a constitution-mandated deliverable and the cheapest way to reduce repeated questions, but it does not block building.

**Independent Test**: Give a reader who has never seen the project the entry-point document only, then ask them to state the project's purpose, its supported stack, and the install, run, migrate, seed, and test commands. All must be answerable from that document alone. Delivers value on its own: a new contributor can orient themselves in minutes.

**Acceptance Scenarios**:

1. **Given** the entry-point document, **When** a new contributor reads it, **Then** they can state the project's purpose, its supported stack, and each standard workflow command without opening another file.
2. **Given** the entry-point document, **When** the contributor looks for the shortcut workflow, **Then** a single documented command is offered for the common path with the equivalent explicit commands shown beside it, and every command in both sections states whether it works today or belongs to the target workflow.
3. **Given** the project's history document, **When** a notable change lands, **Then** the history records that change as part of the same change.

---

### Edge Cases

- The recorded dependency resolution and the dependency declarations disagree — someone hand-edited one, or changed a declaration without re-recording. Installation MUST fail with an actionable message rather than silently resolving different versions. The message MUST include the exact version mismatch and the command needed to correct it.
- A contributor's machine already has a tool of the same name installed globally at a different version. The project-scoped command MUST take precedence without requiring the contributor to uninstall the global one.
- The local configuration file is empty, only partially filled, or contains an unrecognised entry. Documented defaults MUST still apply, and the unknown entry MUST NOT break startup.
- A local configuration file, runtime data directory, cache, or build artifact is created and then staged for commit. It MUST be excluded by the repository's ignore rules.
- The test tree contains no tests yet. The run MUST report success or an explicit, actionable "nothing to run" state — never a silent pass, and never an unexplained failure.
- A contributor proposes a dependency the project already satisfies through a different library, or a second framework for a need the current one already meets. It MUST be rejected unless an explicit justification is recorded.
- The interpreter on the machine is newer or older than the supported baseline. The mismatch MUST be reported with the required version rather than surfacing later as an unrelated failure.
- The interpreter on the machine is newer than 3.12. Installation and the checks MUST succeed, and neither the entry-point document nor the verification output may imply that the newer version has been verified.
- Two contributors on different machines and operating systems install from the same clean checkout and compare their resolved versions. The sets MUST match.
- A contributor runs a convenience command from a directory other than the project root. The behaviour is defined — either it works, or it fails with a clear message — and never a confusing partial run.
- The entry-point document lists a command without an availability marker, or a command listed in the *target workflow* quietly becomes available. The document is non-conforming until the markers are corrected, because a reader cannot tell which commands to run.
- A setting takes effect at runtime but is absent from the configuration template, or a listed setting carries no availability marker. The template is non-conforming until the two lists agree, because a contributor cannot tell which settings have any effect.
- A mandated test area exists on a contributor's machine but holds no tracked file, so it is absent from a fresh clone. The layout is non-conforming until each area is version-controlled.
- The install is run twice in a row, or on a machine where the dependency cache is already warm. The outcome MUST be identical to a first run, with no unexpected upgrades.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The project MUST provide one documented command that installs every runtime and development dependency needed to build, run, and test the project, resolving exactly the versions recorded by the project. A contributor MUST be able to reach a working, verified development environment from a clean checkout using a small number of commands, with no manual interpreter selection, no manual dependency resolution, and no hand-edited files.
- **FR-002**: The project MUST support a single documented interpreter baseline, Python 3.12 or later, and MUST fail with a message naming the required version when the available interpreter does not satisfy it. The foundation MUST be verified on 3.12 only; interpreters above the minimum are accepted but are not verified by this phase and no multi-version support may be claimed on their basis.
- **FR-003**: The recorded dependency resolution MUST be committed to the repository and MUST NOT be hand-edited.
- **FR-004**: Runtime dependencies and development-only dependencies MUST be declared separately, and each MUST be used only in its own context — a development-only dependency MUST NOT be required in order to run the application.
- **FR-005**: Every declared dependency MUST have a recorded technical purpose.
- **FR-006**: A proposed dependency that duplicates a capability the project already provides MUST be rejected unless an explicit justification is recorded.
- **FR-007**: The project MUST provide exactly one importable source package that later features extend, importable from the installed environment without manual path configuration and without environment activation.
- **FR-008**: The project MUST remain a modular monolith. This phase MUST NOT introduce services, message brokers, distributed runtime, event sourcing, or CQRS.
- **FR-009**: The project MUST ship a configuration template covering every setting in the project's published configuration reference, documenting for each one its name, type, default value, purpose, the validation rule applied to constrained values, and an explicit marker stating whether the setting is honoured in the current phase. A setting with no marker is non-conforming.
- **FR-010**: The configuration template MUST NOT contain real secrets, credentials, personal data, or machine-specific absolute paths.
- **FR-011**: Configuration MUST be externalized — supplied through the environment or a local configuration file — rather than hard-coded in source.
- **FR-012**: The project MUST operate on documented defaults when no local configuration file is present.
- **FR-013**: The repository MUST ignore local configuration files, runtime data including the local database, caches, build artifacts, and editor/OS noise, and MUST provide automated or documented checks that prevent these from being committed.
- **FR-014**: Real secrets, real credentials, and personal data MUST NOT appear anywhere in the committed repository or its history.
- **FR-015**: The repository MUST provide an entry-point document stating the project's purpose, its supported stack, and the commands for install, run, migrate, seed, and test, together with the shortcut form of that workflow. The document MUST separate the commands that work today from the target workflow, and every documented command MUST carry an explicit availability marker. A command with no marker is non-conforming.
- **FR-016**: The repository MUST maintain a versioned history document recording notable changes, updated within the same change as the behaviour it records.
- **FR-017**: The repository MUST provide convenience commands that wrap plain commands documented elsewhere, one-to-one, adding no hidden steps and acting as a prerequisite for nothing.
- **FR-018**: The project MUST provide runnable, committed configuration for automated formatting and linting, and for type checking where adopted, so that any contributor can run the same checks locally.
- **FR-019**: The automated checks MUST report a non-zero exit status on failure and MUST identify what needs attention.
- **FR-020**: The test suite MUST be executable from a clean checkout, MUST report success, and MUST NOT report success by having collected nothing.
- **FR-021**: The test tree MUST be organised into the three mandated areas — unit, integration, and API — each of which MUST be a version-controlled location containing at least one tracked file, so the mandated layout is present on a fresh clone.
- **FR-022**: Tests MUST NOT depend on execution order and MUST NOT modify development data.
- **FR-023**: The project MUST provide a documented verification step a contributor can run to confirm their environment matches the documented baseline.


### Key Entities

- **Development Environment**: the complete local setup a contributor works in — interpreter, recorded dependency versions, and the importable source package. Has exactly one documented baseline and one documented way to create it.
- **Configuration Setting**: a single externally supplied value that affects behaviour. Attributes: name, type, default value, purpose, the environments it applies to, whether it is sensitive, and whether it is honoured in the current phase. Never carries a real secret.
- **Dependency Declaration**: a third-party capability the project relies on. Attributes: name, recorded purpose, scope (runtime or development-only), and the reason it is needed. Resolved versions are recorded once, centrally, and committed.
- **Convenience Command**: a named task wrapping a plain documented command. Attributes: name, the plain command it wraps, and its purpose. Never a prerequisite for that plain command.
- **Automated Check**: a runnable verification step such as formatting, linting, type checking, or the test run. Attributes: name, what it reports, and the pass/fail signal it produces.
- **Test Area**: one of the three mandated test categories — unit, integration, or API. Attributes: name, what it covers, and whether it may touch a database or an HTTP layer.
- **Documentation Set**: the entry-point document, the versioned history document, and the design documents. Attributes: name, audience, and the condition that obliges an update.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A contributor starting from a clean checkout on a machine with no project tooling obtains a working, verified development environment using at most two documented commands and under 10 minutes on a standard broadband connection.
- **SC-002**: 100% of clean-checkout installs on the supported interpreter baseline succeed on the first attempt with no manual dependency intervention; zero environment-setup failures are attributable to missing or undocumented configuration.
- **SC-003**: Two contributors on different machines and operating systems resolve the same version of every declared dependency from the same clean checkout — a 100% match, with zero version drift.
- **SC-004**: 100% of declared dependencies have a recorded technical purpose; the count of dependencies added without a stated purpose is zero.
- **SC-005**: The full automated check run from a clean checkout completes successfully in under 60 seconds, with zero failures and zero "nothing collected" outcomes.
- **SC-006**: Every deliberately introduced violation — formatting drift, a failing test, an unused dependency, a staged local configuration file — is reported as a failure by the automated checks or the documented review steps in 100% of cases.
- **SC-007**: 100% of the settings in the published configuration reference appear in the configuration template with name, type, default, purpose, validation rule, and an availability marker, so a contributor can answer "what can I configure, what happens if I get it wrong, and which of those take effect today" from that one file.
- **SC-008**: A scan of the committed repository and its history finds zero real secrets, real credentials, personal data, or committed local configuration files.
- **SC-009**: 100% of the documented convenience commands map one-to-one to a plain command a contributor can run directly with identical results; zero steps exist only inside the convenience layer.
- **SC-010**: A reader given only the entry-point document can state the project's purpose, its supported stack, and the install, run, migrate, seed, and test commands without opening another file, and correctly identifies which of those commands work today — confirmed for 4 of 5 first-time readers.
- **SC-011**: The foundation installs and passes its checks on the declared minimum interpreter, 3.12, with zero failures, and in 100% of attempts on a machine below that minimum the contributor receives a failure message naming the required version rather than a downstream or unrelated error.
- **SC-012**: A fresh clone contains all three mandated test areas, each with at least one tracked file, verified on a clean checkout with zero missing areas.

## Assumptions

- The technology baseline — language, web framework, data-modelling and migration tooling, dependency manager, and test tooling — is already fixed by `.specify/memory/constitution.md` v1.1.1 and `docs/plan.md` §2. This specification does not choose or revisit it. It specifies only the observable outcome: the project installs, configures, verifies, and tests reproducibly. Command names appear here solely because `docs/plan.md` §6 states the install command as the phase's completion criterion, making it the observable acceptance contract rather than a design choice made here.
- Version control already exists and the repository is tracked; "initialize the repository" in the plan is read as establishing the tracked project layout and its ignore rules, not creating version control from scratch.
- Scope boundaries. This specification covers `docs/plan.md` §6 (Phase 0) only. Out of scope: the web application, the health endpoint, runtime settings loading and startup validation, the database baseline and migrations, any business domain, the fake-data generator, failure simulation, authentication, observability, and any CI pipeline. If a pipeline is added it MUST consume the same documented commands and MUST NOT introduce a second dependency manager or build system.
- This directory is the single foundation feature, numbered `001`. Where this specification covers Phase 0 (`docs/plan.md` §6) and the wider foundation across Phases 0–2, this feature owns the runtime concerns too: the web application, the health endpoint, runtime settings loading and validation, and the migration baseline. It MUST NOT pre-empt them; those are recorded here as later phases of the same feature, not as a separate one.
- Constitution divergence, recorded rather than silently resolved. `.specify/memory/constitution.md` v1.1.1, in *Speckit Development Process*, still enumerates `001-foundation` … `011-observability` as the feature layout. That enumeration is read as an illustrative roadmap, not a binding directory list, and the divergence is carried as a deferred conflict to be resolved when the second feature is specified. Until then, feature directories take the next free number (`002`, `003`, …) and MUST NOT assume a reserved slot exists.
- The linting, formatting, and type-checking tool selection is already recorded as decided in `docs/development.md` (W18). This specification requires only that the choice is committed, configured, and runnable by any contributor — it does not mandate a particular tool.
- The three mandated test areas are created as locations from this phase even though only unit-level checks are meaningful before any application code exists; the integration and API areas are populated by later features.
- Users of this feature are contributors and automated verification — a human developer or a reviewer/CI job running the documented commands. No end-user-facing behaviour is delivered, so all acceptance is by the people operating the project.
- The dependencies available to this phase are those named in `docs/plan.md` §6: fastapi, uvicorn, pydantic, pydantic-settings, sqlalchemy, alembic, pytest, httpx, faker. Each MUST have a recorded purpose per FR-005; a dependency not needed until a later phase MAY be declared now only with that recorded purpose.
- Contributors are assumed to have a supported interpreter, network access for the first install, and version control installed. Offline or air-gapped installation is not a requirement of this phase, but a warm-cache re-install MUST produce the same outcome as a first install. Verification is performed on 3.12 only; a contributor on a newer interpreter is expected to succeed but is not a supported verification target.
- "Identical versions" in SC-003 means the same resolved version of every declared dependency across contributors, not byte-identical resolution artifacts across differing platforms.
- Platform-specific differences — operating system, path separators, SQLite behaviour — are handled in later phases. This phase requires only that they be documented rather than discovered by surprise.
