<!--
Sync Impact Report
Version change: unversioned template -> 1.0.0
Modified principles: Template Principle 1 -> MVP Slice First; Template Principle 2 -> Canonical Data Ownership; Template Principle 3 -> API Contract Discipline; Template Principle 4 -> Quality Gates Are Non-Negotiable; Template Principle 5 -> UI, Security, and Operations Discipline
Added sections: Scope and Release Rules; Development Workflow and Governance
Removed sections: none
Templates updated: ✅ .specify/templates/plan-template.md; ✅ .specify/templates/tasks-template.md
Follow-up TODOs: none
-->

# BorneMap Constitution

## Core Principles

### I. MVP Slice First
Every approved increment MUST deliver a complete, usable slice of BorneMap.
MVPs are sequenced to prove one product loop at a time, and later MVPs MUST
preserve earlier behavior unless an ADR explicitly approves a breaking change.
New infrastructure or platform layers MUST not be introduced before the current
MVP needs them. Rationale: the roadmap depends on fast validation without
overbuilding.

### II. Canonical Data Ownership
`inventory.station` is the source of truth for stations. `gis` MUST never own a
business entity, and `analytics` MUST store reporting data only. SQL MUST use
bind parameters. Public API identifiers MUST NOT be sequential integers.
Rationale: clear ownership prevents drift between operational, spatial, and
reporting data.

### III. API Contract Discipline
All services and endpoints MUST live under `/api`. `GET /api/health` MUST
perform a database check. MVP-1 endpoints MUST remain unauthenticated, and any
API change MUST keep the documented resource set consistent with
`docs/api/README.md` until a versioned contract is approved. Rationale: a stable
public contract keeps driver, dashboard, and service integrations predictable.

### IV. Quality Gates Are Non-Negotiable
Changes that touch runtime behavior MUST be validated against a real database.
Driver apps MUST fail gracefully when the API is unreachable. Every MVP close
MUST clear zero Class A bugs, complete API documentation, and pass the manual
smoke test. Rationale: the platform only ships when correctness and
recoverability are demonstrated.

### V. UI, Security, and Operations Discipline
Frontend code MUST use design tokens only; no hardcoded visual values are
allowed. Arabic RTL MUST be correct on every screen starting in MVP-3. Public
browsing MUST never trigger an auth prompt. Secrets MUST never be committed,
MVP-1 runtime code MUST stay under `source/`, MVP-1 local development MUST use
Dockerfiles and local Docker Compose, Keycloak arrives in MVP-3, and Traefik
public exposure arrives in MVP-6. Rationale: consistent UX, safe secrets
handling, and staged operations keep the platform maintainable.

## Scope and Release Rules

The product serves Public Drivers, Registered Drivers, Partners, and Admins as
defined in `docs/constitution.md`. MVP-1 covers station discovery, partner and
admin management, and manual availability updates. The explicit exclusions in
`docs/out-of-scope-registry.md` remain excluded until a later MVP or approved
ADR. MVP planning MUST preserve earlier user flows when later MVPs add new
capability.

## Development Workflow and Governance

Plans and specs MUST include a constitution check before work starts and again
after design changes. User stories MUST be independently testable and ordered by
priority. Task breakdowns MUST preserve story boundaries and use the documented
MVP structure. Any amendment MUST include a rationale, a version bump, and an
updated sync report. Compliance is reviewed during planning, before merge, and
at MVP close.

## Governance

This constitution supersedes conflicting planning guidance, templates, and
working notes. Amendments require documented intent, a semantic version update,
and propagation to dependent templates or docs when the change affects them.
Versioning follows semantic rules: MAJOR for incompatible principle or section
changes, MINOR for new or materially expanded guidance, and PATCH for
clarifications or wording fixes. If a ratification date is unknown, record it as
`TODO(RATIFICATION_DATE)` until it can be confirmed. Current compliance is
verified by checking feature plans, task lists, API docs, and runtime guidance
against these principles.

**Version**: 1.0.0 | **Ratified**: 2026-06-08 | **Last Amended**: 2026-06-08
