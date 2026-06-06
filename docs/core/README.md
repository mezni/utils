# Core Documentation

Essential project documentation defining the platform's purpose, principles, scope, and governance.

## Contents

- **[Constitution](constitution.md)** — Non-negotiable principles, rules, and structures that govern BorneMap. Must never be violated. Covers core principles, service inventory, data architecture, roles & access, authentication, frontend apps, design system, and bug classification.

- **[Scope](scope.md)** — What the platform includes and what is explicitly deferred or out of scope. Covers included features (discovery, driver features, partner features, admin features), explicitly deferred components (OCPP, payments, routing, real-time availability, notifications), infrastructure choices, and regulatory compliance.

- **[Glossary](glossary.md)** — Essential terminology used throughout BorneMap. Covers user roles, core entities, authentication & authorization, services & architecture, data & database, frontend & design, operations & deployment, analytics, quality & testing, GIS & spatial, and common abbreviations.

- **[Implementation Plan](implementation-plan.md)** — Phase-by-phase breakdown of work. (To be updated as phases are planned.)

- **[Decisions Log](decisions.md)** — Small operational decisions and tracking that don't require ADRs. (To be updated as decisions are made.)

## Project Management

### Backlog
- [Backlog](project/backlog.md) — All pending work items
- [Bugs](project/bugs.md) — Reported issues and bug tracking
- [Roadmap](project/roadmap.md) — Timeline and phase deliverables
- [Sprints](project/sprints/) — Sprint planning, standups, retrospectives

## Key Rules

From the Constitution:

1. **Pragmatic Architecture** — Minimum services, clear responsibilities
2. **Single Source of Truth** — Every entity has exactly one authoritative owner
3. **Simple Operations** — One person must be able to operate the platform
4. **Domain Separation by Schema** — Business, GIS, users, analytics are separated
5. **Build for Current Scale** — No premature optimization
6. **Public Access First** — No login required to browse stations
7. **RTL & Arabic Built-In** — Not an afterthought
8. **Visual Consistency** — Tokens define all visual values

## Critical Non-Negotiable Rules (Class A)

These must never be violated:

- **inventory.station** is the source of truth for stations
- **Public access** never requires login
- **Tokens** never stored in localStorage or AsyncStorage
- **Arabic RTL** must work on every screen
- **Only Traefik** exposes public ports
- **Keycloak** owns all authentication
- **No additional services** without an approved ADR

## Getting Started

1. **New to BorneMap?** Start with [Constitution](constitution.md) section 1 (Core Principles) and [Glossary](glossary.md)
2. **Need to understand the system?** Read [Architecture Overview](../architecture/overview.md)
3. **Working on a specific area?** Check the relevant documentation section
4. **Making architecture decisions?** Check [ADRs](../adr/) first, then propose a new one if needed

---

**Last Updated:** 2026-06-05
