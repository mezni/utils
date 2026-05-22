# BorneMap Methodology

How work is organized, tracked, and validated. This document defines the
**process layer** that sits on top of the [roadmap](./roadmap.md) and is
constrained by the [Constitution](../.specify/memory/constitution.md).

## Workflow: Kanban with sprint cadence

- **Kanban** is the day-to-day workflow: pull-based, work-in-progress
  limits, no fixed iteration commitments.
- **Sprint-style delivery phases** map to the roadmap phases. Each phase
  is a sprint band (1–4 weeks). A phase ends only when its Definition of
  Done (Constitution Principle VII) is met.

### Kanban columns

| Column | Definition | WIP limit (solo) |
|---|---|---|
| Backlog | Idea / not yet refined | ∞ |
| Ready | Refined, acceptance criteria written, linked to a roadmap phase | 5 |
| In Progress | Actively being worked | 1 |
| In Review | PR open, awaiting checks + review | 2 |
| Blocked | Waiting on external dependency / decision | 3 |
| Done | Merged to trunk, DoD satisfied | ∞ |

WIP limits are advisory for a solo developer but MUST tighten as the team
grows. Adjust via PR to this file.

### Card lifecycle

1. Created in **Backlog** with a one-line problem statement.
2. Refined into **Ready** when it has: phase tag, principle tag(s),
   acceptance criteria, test plan.
3. Pulled into **In Progress** (≤ 1 at a time, solo).
4. PR opened → moves to **In Review**.
5. Merge → **Done** only if Constitution Principle VII DoD holds.

## GitHub Projects schema

A single GitHub Project (board view) tracks all work.

**Required custom fields**

| Field | Type | Values |
|---|---|---|
| Status | Single-select | Backlog, Ready, In Progress, In Review, Blocked, Done |
| Phase | Single-select | 0, 1, 2, 3, 4, 5, 5.5, 6, 7, 8, 9, 10, 11, 12 |
| Principle | Multi-select | I, II, III, IV, V, VI, VII |
| Type | Single-select | feature, bug, chore, docs, adr, security, perf, test |
| Size | Single-select | XS, S, M, L, XL |

**Required views**

- **Board (by Status)** — the primary Kanban view.
- **By Phase** — table grouped by Phase to see roadmap burn-down.
- **Blocked** — filter Status = Blocked.
- **Security / Audit** — filter Type ∈ {security} OR Principle contains V.

Cards without `Phase` and at least one `Principle` MUST NOT leave Ready.

## Branch strategy

Trunk-based development.

- `main` is always deployable.
- Feature branches: `phase-<N>/<short-slug>` (e.g.,
  `phase-5/outbox-relay-worker`).
- ADR branches: `adr/<number>-<short-slug>`.
- Hotfix branches: `hotfix/<short-slug>`.
- Branches are short-lived (target ≤ 3 days open) and rebased onto
  `main` before merge.
- Squash merge is the default. The PR title becomes the commit subject;
  follow Conventional Commits (`feat:`, `fix:`, `docs:`, `chore:`,
  `refactor:`, `test:`, `perf:`, `ci:`).

## Pull request rules

Every PR MUST:

1. Reference a Project card and roadmap phase.
2. Cite the Constitution principle(s) it upholds (or the ADR that
   permits a deviation).
3. Pass all CI jobs (lint → unit → integration → openapi-bundle →
   docker-build).
4. Update OpenAPI if a REST surface changed.
5. Include or update tests for the categories mandated by Principle VII.
6. Be reviewed and approved per `CODEOWNERS` before merge.

PR description template (lives at `.github/PULL_REQUEST_TEMPLATE.md`):

```
## Summary
<what + why>

## Phase / Principles
- Phase: <N>
- Principles: <I..VII>

## Changes
- ...

## Tests
- [ ] unit
- [ ] integration
- [ ] transaction (if mutation)
- [ ] outbox (if event-producing)
- [ ] audit (if auditable)
- [ ] soft-delete (if infrastructure entity)
- [ ] spatial (if geo)

## DoD
- [ ] Tests passing
- [ ] OpenAPI updated (or N/A)
- [ ] Security validated
- [ ] Logging/metrics/health verified
- [ ] ADR filed (if constitutional boundary affected)
```

## GitHub Actions CI/CD

A single workflow matrix runs on every push and PR. Jobs MUST be parallel
where possible and fail fast.

### Pipeline

```text
            ┌── lint ─────────────────┐
push/PR ────┤                         ├── docker-build ── (push images on main only)
            ├── unit ─────────────────┤
            ├── integration ──────────┤
            ├── openapi-bundle ───────┤
            └── (release on tag) ─────┘
```

### Required jobs

| Job | Purpose | Triggers |
|---|---|---|
| `lint` | ESLint (TS), Clippy (Rust), Prettier, markdownlint | every PR/push |
| `unit` | NestJS + Rust unit tests | every PR/push |
| `integration` | Docker-compose-backed integration tests (Postgres, Mongo, RabbitMQ) | every PR/push |
| `openapi-bundle` | Bundle + validate OpenAPI; fail if drift vs committed spec | every PR/push |
| `docker-build` | Build all service images; push to registry only on `main` | every PR (build), `main` (push) |
| `release` | Tag-driven; build, tag images, attach changelog | tag push `v*` |

### Branch protection (target state on `main`)

- Require status checks: `lint`, `unit`, `integration`, `openapi-bundle`,
  `docker-build`.
- Require linear history.
- Require code-owner review.
- Restrict who can push (no direct push to `main`).

## Releases

- Versioning: semantic versioning at the repository level (`vMAJOR.MINOR.PATCH`).
- Tag → triggers `release` job → produces deployable images.
- Each release notes file lives under `docs/releases/<version>.md` (created
  on first release; not part of the initial docs commit).

## Definition of Ready (Ready column)

A card MUST satisfy all of the following before it can be pulled In Progress:

- [ ] Linked to a roadmap phase
- [ ] Tagged with at least one Constitution principle
- [ ] Acceptance criteria written (Given/When/Then bullets)
- [ ] Test plan listed (categories from Principle VII)
- [ ] Out-of-scope items explicitly listed
- [ ] If it touches a constitutional boundary: ADR drafted

## Definition of Done (Done column)

Inherits Constitution Principle VII verbatim:

1. All applicable tests pass in CI.
2. OpenAPI specs updated for any REST surface change.
3. Security review confirms Principle V is upheld.
4. Logging/metrics/health for the changed path comply with Principle VI.
5. An ADR is filed if a constitutional boundary is affected.
