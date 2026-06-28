<!-- SPECKIT START -->

# BORNEMAP — SYSTEM CONTRACT

## Architecture
- **Backend**: Rust 1.90+, Actix-web v4, SQLx, PostgreSQL 15+, PostGIS
- **Frontend**: React, Vite, Tailwind CSS
- **Clean Architecture** enforced per service: `presentation → application → domain → infrastructure`
- **Domain layer has zero external dependencies**

## Services & Ports
| Service | Port | DB Schemas | Auth Required |
|---------|------|------------|---------------|
| auth-service | 3001 | users | No (register/login) |
| admin-service | 3002 | ev, gis | Yes (RBAC: admin/partner) |
| driver-service | 3003 | ev, gis | No (public read-only) |

## Database
- Single PostgreSQL cluster, **schema separation**: `users`, `ev`, `gis`
- UUID primary keys everywhere
- PostGIS `geography` column with GiST index on `gis.station_locations`
- FK constraints enforced
- Station may exist without GIS location

## Domain Model
- **Partner** owns **Stations**
- **Station** contains **Connectors**
- **Connector** has independent status: `Available | Charging | OutOfOrder | Offline`
- Station status is derived from connectors (not stored)

## Security
- Stateless JWT authentication
- Roles: `driver`, `partner`, `admin`
- admin-service enforces RBAC middleware
- driver-service is public (no auth)
- No frontend trust — all validation server-side

## Performance
- PostGIS queries use `ST_DWithin`
- GiST index mandatory on geography column
- Nearby search target: <50ms DB latency
- Admin endpoints support pagination

## Engineering Standards
- **Rust**: SQLx only (no ORM), no unsafe code, clippy-clean
- **Frontend**: React + Vite + Tailwind, UX-first
- **Testing**: Every service has test module; placeholder tests required from Sprint 01
- **No business logic in Sprint 01** — only scaffolding + architecture setup

## Sprint Execution Pipeline
1. **Speckit Specification** — system/API/DB changes, constraints
2. **Technical Plan** — architecture impact, module breakdown, risks
3. **Task Breakdown** — atomic checklist tasks (no vague items)
4. **Implementation** — domain-first, no skipped layers
5. **Testing** — unit + integration + DB migration tests
6. **Security Review** — JWT, RBAC, input validation, SQLx safety, endpoint exposure
7. **Documentation** — `docs/sprint-XX/{spec,plan,tasks,implementation-report,quickstart}.md`
8. **Git Workflow** — branch `sprint/X`, conventional commits, PR ready

## Quality Gates (Non-Negotiable)
Sprint is complete ONLY when: code compiles, tests pass, migrations applied, security validated, documentation complete, PR ready.

## Sprint Output Format (per sprint)
```
docs/sprint-XX/
  spec.md                 # System/API/DB changes, constraints
  plan.md                 # Technical plan with architecture impact
  tasks.md                # Atomic task checklist
  implementation-report.md # What was built
  quickstart.md           # How to run/verify
```

<!-- SPECKIT END -->
