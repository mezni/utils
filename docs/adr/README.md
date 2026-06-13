# ADR - Architecture Decision Record

## Overview

This directory contains all Architecture Decision Records (ADRs) for the BorneMap project.

## What is an ADR?

An ADR is a short, written document that describes a significant architectural decision, including:

- Context (what problem are we solving?)
- Decision (what did we decide?)
- Rationale (why did we make this decision?)
- Consequences (what are the trade-offs?)
- Alternatives (what else did we consider?)
- Status (accepted, deprecated, superseded?)

## ADR Template

```markdown
# ADR-XXX: [Title]

## Status

[ACCEPTED] | [REJECTED] | [SUPERSEDED] | [PROPOSED]

## Context

[Describe the context and problem we're solving]

## Decision

[Describe the decision we made]

## Rationale

[Why did we make this decision? What problem does it solve?]

## Consequences

[What are the consequences of this decision?]

## Alternatives Considered

[What other options did we consider? Why did we reject them?]

## Related ADRs

[ADR-XXX, ADR-YYY, etc.]

## Status

[ACCEPTED] | [REJECTED] | [SUPERSEDED] | [PROPOSED]
```

## ADR Categories

### Design Decisions
- UI/UX design
- Component architecture
- State management
- Data flow

### Technical Decisions
- Technology stack
- Framework choices
- Database choices
- API design

### Process Decisions
- Development workflow
- Testing strategy
- Code review process
- Deployment process

### Security Decisions
- Authentication
- Authorization
- Data protection
- API security

## ADR Lifecycle

1. **PROPOSED**: Decision proposed, not yet accepted
2. **ACCEPTED**: Decision accepted, no longer changes
3. **SUPERSEDED**: Decision superseded by another ADR
4. **REJECTED**: Decision rejected

## ADR Directory Structure

- `/adr/`: Main ADR directory
- `/adr/*.md`: Individual ADR files (naming: ADR-001.md, ADR-002.md, etc.)

## ADR Numbering

- **001-099**: Core architectural decisions
- **100-199**: UI/UX decisions
- **200-299**: Technical infrastructure decisions
- **300-399**: Process and workflow decisions
- **400-499**: Security decisions
- **500-599**: Data management decisions

## Creating a New ADR

1. Create a new file: `docs/adr/ADR-XXX.md`
2. Follow the ADR template
3. Update this README with the new ADR
4. Share with the team for review
5. Move to ACCEPTED or REJECTED status

## Review Process

- All ADRs must be reviewed before acceptance
- ADRs must follow the ADR template
- ADRs must be linked to related ADRs
- ADRs must document alternatives considered
- ADRs must document consequences

## Related Documentation

- Constitution: `/docs/01_constitution.md`
- Agents: `/docs/02_agents.md`
- Architecture: `/docs/architecture/`
- Specs: `/docs/specs/`
- Bugs: `/docs/bugs/`

## ADR Index

- [ADR-001: React Query for Server State](ADR-001.md)
- [ADR-002: Rust for Backend Services](ADR-002.md)
- [ADR-003: MapContainer Abstraction](ADR-003.md)
- [ADR-004: PostGIS for Geospatial Data](ADR-004.md)

---

*Architecture decisions drive system evolution and prevent architectural drift.*