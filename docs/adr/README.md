# Architecture Decision Records (ADRs)

This directory contains all Architecture Decision Records for the BorneMap platform. ADRs document major architecture decisions and their rationale.

---

## ADR Index

| ID | Title | Status | Date |
|----|----|--------|------|
| [ADR-001](adr-001-single-database.md) | PostgreSQL + PostGIS as Single Database | Accepted | 2026-01-15 |
| [ADR-002](adr-002-schema-separation.md) | Schema Separation Over Database Separation | Accepted | 2026-01-16 |
| [ADR-003](adr-003-prefixed-nanoids.md) | Prefixed NanoIDs Over UUIDs | Accepted | 2026-01-17 |
| [ADR-004](adr-004-direct-analytics.md) | Direct Analytics Insert Over RabbitMQ | Accepted | 2026-01-18 |
| [ADR-005](adr-005-rust-backend.md) | Rust for Backend Services | Accepted | 2026-01-20 |
| [ADR-006](adr-006-docker-compose.md) | Bare Metal + Docker Compose Over Kubernetes | Accepted | 2026-01-22 |
| [ADR-007](adr-007-keycloak-auth.md) | Keycloak for Authentication | Accepted | 2026-01-25 |
| [ADR-008](adr-008-gis-trigger.md) | PostgreSQL Trigger for GIS Synchronization | Accepted | 2026-01-27 |
| [ADR-009](adr-009-monorepo.md) | Monorepo with Cargo and npm Workspaces | Accepted | 2026-02-01 |
| [ADR-010](adr-010-traefik.md) | Traefik as Edge Router | Accepted | 2026-02-03 |
| [ADR-011](adr-011-react-vite.md) | React + Vite for Web Applications | Accepted | 2026-02-05 |
| [ADR-012](adr-012-react-native.md) | React Native + Expo for Mobile App | Accepted | 2026-02-07 |
| [ADR-013](adr-013-single-dashboard.md) | Single Dashboard App Over Separate Partner and Admin Apps | Accepted | 2026-02-10 |

---

## ADR Process

### Creating an ADR

An ADR is required for decisions that:
- Introduce a new service, infrastructure component, or data store
- Change the source of truth for any entity
- Change the authentication or authorization model
- Supersede a previous ADR
- Introduce a pattern not currently in use

**Steps:**
1. Create `adr-NNN-title.md` using the template below
2. Write the decision and rationale
3. Get stakeholder approval
4. Commit to `docs/adr/`
5. Reference the ADR number in relevant documentation

### ADR Template

```markdown
# ADR-NNN: Title

**Status:** Accepted | Pending | Superseded  
**Decision Date:** YYYY-MM-DD  
**Supersedes:** (if applicable)  
**Superseded By:** (if applicable)  

## Context

[Describe the issue or problem being addressed]

## Decision

[State the decision clearly]

## Rationale

[Explain why this decision was made]

## Consequences

[Describe the positive and negative consequences]

## Alternatives Considered

[List and briefly explain alternatives]

## Related Decisions

[Reference other ADRs if applicable]
```

### Updating an ADR

**ADRs are never edited after acceptance.** If a decision changes:
1. Mark the old ADR with status "Superseded By: ADR-XXX"
2. Create a new ADR referencing the old one
3. Update relevant documentation

### ADR Lifecycle

- **Pending** — Draft, under discussion
- **Accepted** — Decision made, implemented or ready to implement
- **Superseded** — Decision changed, see newer ADR

---

## Decision Categories

### Infrastructure (ADR-006, ADR-010)
- Deployment platform
- Edge routing and TLS

### Data & Storage (ADR-001, ADR-002, ADR-003, ADR-004, ADR-008)
- Database technology and schema design
- Identifier schemes
- Analytics architecture

### Backend Services (ADR-005)
- Language and framework choices

### Authentication (ADR-007)
- Authentication and authorization

### Frontend (ADR-011, ADR-012, ADR-013)
- Web application technology
- Mobile application technology
- Multi-role dashboard design

### Organization (ADR-009)
- Monorepo structure and tooling

---

## Key Principles Reflected in ADRs

1. **Pragmatic Architecture** — Minimum services, clear responsibilities
2. **Single Source of Truth** — Each entity has one authoritative owner
3. **Simple Operations** — One person can operate the platform
4. **Build for Current Scale** — No premature optimization
5. **Domain Separation** — Schema-based isolation of concerns

---

## Questions?

1. Review the relevant ADR for rationale on a specific decision
2. Check the [Constitution](../core/constitution.md) for non-negotiable rules
3. Review [Architecture Overview](../architecture/overview.md) for system design
4. See related ADRs for context on interconnected decisions

---

**Last Updated:** 2026-06-05
