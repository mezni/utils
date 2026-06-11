# ADR-006: Source Root Monorepo

**Status:** Accepted
**Date:** 2026-06-10
**MVP:** MVP-1

---

## Context

BorneMap includes frontend apps, backend services, infrastructure config, and documentation. The team needed a repository structure that enforces clear separation between runtime code and supporting files.

## Decision

**All runtime code under `/source` monorepo root.**

```
/ (repo root)
├── source/          ← ALL runtime code
│   ├── services/    ← Backend microservices + shared libs
│   │   ├── libs/    ← Shared libraries (e.g. borne-data)
│   │   ├── driver-service/
│   │   ├── admin-service/
│   │   └── clickstream-service/
│   ├── frontend/    ← Frontend applications
│   │   ├── mobile-driver/
│   │   ├── web-driver/
│   │   └── dashboard/
│   └── packages/    ← Shared workspace packages
│       ├── ui/
│       ├── design-tokens/
│       ├── api-contracts/
│       ├── types/
│       ├── event-taxonomy/
│       ├── config/
│       └── utils/
├── docs/            ← architecture, ADRs, specs
├── infra/           ← Docker, Traefik, deployment
└── scripts/         ← dev tooling
```

## Rationale

- Single source of truth for all application code
- Clear boundary: nothing outside /source is runtime or deployable
- Tooling scripts and infra config are separate concerns
- Prevents accidental deployment of non-runtime files
- Simplifies CI/CD path glob patterns

## Consequences

- No runtime code may exist outside /source
- /docs, /infra, /scripts are supportive only
- Monorepo tooling (pnpm workspaces) scoped to /source
- Build and deploy pipelines reference /source exclusively for application artifacts

## Related

- ADR-004: Microservice Boundaries
