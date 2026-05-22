# BorneMap Documentation

Navigation index for the BorneMap project documentation.

BorneMap is a geospatial EV charging discovery platform for Tunisia. This
directory holds everything that is not code: governance, plans, architecture,
operations, and decision records.

## Project Governance

- **Constitution** — `../.specify/memory/constitution.md`
  The non-negotiable rules of the platform (Principles I–VII, technology
  stack, deployment topology, amendment policy). All other documents in this
  tree MUST comply with the constitution.

## Plans

- [Roadmap](./roadmap.md) — phased delivery plan (Phase 0 → Phase 12) with
  goals, deliverables, and the principles each phase satisfies.
- [Methodology](./methodology.md) — Kanban workflow, sprint cadence,
  GitHub Projects schema, and GitHub Actions outline.

## Architecture

- [Architecture overview](./architecture/overview.md) — service map,
  outbox sequence, and PKCE auth flow (with Mermaid diagrams).

## Architecture Decision Records (ADRs)

- [ADR index](./adr/README.md)
- [ADR template](./adr/template.md) (MADR-style)

Accepted ADRs (binding at constitution ratification):

- [ADR-0001 — Companies as top-level grouping](./adr/0001-companies-as-top-level-grouping.md)
- [ADR-0002 — geo-service implemented in Rust](./adr/0002-geo-service-in-rust.md)
- [ADR-0003 — Keycloak as sole identity provider](./adr/0003-keycloak-sole-idp.md)
- [ADR-0004 — Outbox pattern for reliable event publishing](./adr/0004-outbox-pattern.md)
- [ADR-0005 — Soft delete on infrastructure entities only](./adr/0005-soft-delete-infrastructure-only.md)

## Operations

- [Deployment](./operations/deployment.md) — Docker Compose topology,
  NGINX routing, TLS, environment-variable strategy, `/health` and
  `/metrics` contract.

## How to contribute documentation

1. If your change affects a **constitutional boundary** (service
   responsibility, data ownership, identity provider, event pipeline,
   soft-delete scope, deployment topology, or approved stack), file an ADR
   first using [`adr/template.md`](./adr/template.md). See
   Constitution §"Development Workflow & Governance".
2. Keep prose declarative and testable. Prefer MUST / SHOULD / MUST NOT
   over "should probably".
3. Cross-link to the constitution principle each rule enforces.
4. Diagrams use Mermaid (renders natively on GitHub).
