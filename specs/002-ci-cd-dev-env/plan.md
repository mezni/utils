# Implementation Plan: Phase 1 — CI/CD & Dev Environment

**Branch**: `002-ci-cd-dev-env` | **Date**: 2026-05-22 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/002-ci-cd-dev-env/spec.md`

## Summary

Ship the automated validation and local development infrastructure that every later phase depends on: a GitHub Actions CI pipeline (lint, unit, integration, OpenAPI bundle, docker-build), a docker-compose local stack with all services and infrastructure containers, observability endpoints (health + metrics) on every service, automated DCO sign-off enforcement, stale-branch flagging, NGINX gateway routing, a Makefile for common tasks, and environment configuration via `.env.example` + GitHub secrets. No application business logic — that is Phase 2+.

Technical approach: deliver GitHub Actions workflow files, Docker Compose configuration, NGINX config, a Makefile, and `.env.example` at fixed repository paths. Infrastructure services use official Docker images; service-specific Dockerfiles are created in their respective phases. CI runs on GitHub-hosted runners using service containers for integration test dependencies. Docker images are published to GitHub Container Registry (GHCR) on merge to `main`.

## Technical Context

**Language/Version**: YAML (GitHub Actions), Shell (Makefile), HCL/conf (NGINX), Markdown (documentation), Dockerfile

**Primary Dependencies**: GitHub Actions (CI platform), Docker Compose (local orchestration), NGINX (gateway), GitHub Container Registry (image storage)

**Storage**: N/A — no persistent storage for CI/CD infrastructure. Docker volumes for local dev databases.

**Testing**: Manual verification per `quickstart.md` runbook. CI pipeline is self-validating: CI jobs test the pipeline itself by running lint, unit, integration, OpenAPI bundle, and docker-build against a reference branch.

**Target Platform**: GitHub.com (CI), Linux x86_64 (GitHub-hosted runners), developer machines running Linux or macOS with Docker installed

**Project Type**: CI/CD pipeline + local development environment (infrastructure/documentation feature)

**Performance Goals**: CI pipeline completes in under 10 minutes for a typical change (SC-001). Local stack starts and passes health checks in under 2 minutes (SC-002).

**Constraints**:
- All CI MUST use GitHub-native mechanisms (GitHub Actions, GHCR). No external CI platform.
- Docker is the only local runtime dependency; no service-specific installations on developer machines.
- Secrets MUST NOT be committed to the repository; managed via GitHub Actions encrypted secrets.
- The `main` branch protection ruleset from Phase 0 MUST be updated to require Phase 1 CI status checks.

**Scale/Scope**: Solo to small team (≤5 Maintainers). Single repository. 4 backend services + frontend scaffold. 6 GitHub Actions workflow files, 1 docker-compose.yml, 1 NGINX config, 1 Makefile, 1 `.env.example`.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Constitution v1.0.0 principles I–VII evaluated against this feature:

| Principle | Applicability | Compliance |
|-----------|--------------|-----------|
| I. Clean Modular Architecture | CI pipeline validates each service independently; local stack runs all services as separate containers | ✅ pass — CI enforces modular boundaries |
| II. Domain Model Integrity | N/A — no domain entities shipped | ✅ pass (vacuous) |
| III. Event Integrity via Outbox | N/A — no event production in this phase | ✅ pass (vacuous) |
| IV. Soft-Delete Discipline | N/A — no DB schema changes | ✅ pass (vacuous) |
| V. Security & Identity | CI manages secrets via GitHub encrypted secrets; DCO enforcement as required status check; no secrets committed to repo | ✅ pass — strengthens security posture from Phase 0 |
| VI. Observability | Every service MUST expose `/health` and `/metrics` endpoints per FR-011, FR-012 | ✅ pass — directly implements Principle VI |
| VII. Quality & Testing Discipline (DoD) | CI pipeline enforces all DoD gates: tests, OpenAPI validation, security review sign-off, observability checks | ✅ pass — CI automates the DoD enforcement |

ADR Governance Rule: this feature does not affect any constitutional boundary (no service responsibility, data ownership, identity provider, event pipeline, soft-delete scope, deployment topology, or approved stack change). No new ADR required.

**Result: PASS. No gate violations, no complexity-tracking entries.**

## Project Structure

### Documentation (this feature)

```text
specs/002-ci-cd-dev-env/
├── plan.md              # This file (/speckit.plan output)
├── spec.md              # Feature specification (/speckit.specify output)
├── research.md          # Phase 0 — research findings
├── data-model.md        # Phase 1 — CI/CD entities
├── quickstart.md        # Phase 1 — verification runbook
├── contracts/           # Phase 1 — CI workflow contracts, NGINX contract
├── checklists/
│   └── requirements.md  # spec quality checklist
└── tasks.md             # Phase 2 — NOT created by /speckit.plan
```

### Repository artefacts produced by this feature

This feature ships files at fixed repository paths. The full target inventory:

```text
bornemap/                              (repository root)
├── .github/
│   └── workflows/
│       ├── ci.yml                     # NEW — main CI pipeline
│       ├── dco.yml                    # NEW — DCO sign-off check
│       └── stale-branches.yml         # NEW — weekly stale branch scan
├── docker-compose.yml                 # NEW — local dev stack
├── docker-compose.override.yml        # NEW — local dev overrides
├── Makefile                           # NEW — dev workflow commands
├── .env.example                       # NEW — environment template
├── nginx/
│   └── default.conf                   # NEW — NGINX gateway config
├── services/                          # exists (empty), gets service scaffolds in Phase 2+
└── frontend/                          # exists (empty), gets scaffold in Phase 2
```

Configuration applied **outside repository contents** (GitHub repo settings):

- Update `main` branch protection ruleset to require Phase 1 status checks (lint, unit, integration, openapi-bundle, docker-build, DCO).
- Configure GitHub Actions secrets for GHCR authentication.

## Complexity Tracking

> No Constitution Check violations. This table intentionally left empty.

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| _(none)_  | _(none)_   | _(none)_                            |
