# ADR-007: Source-Rooted Codebase

**Date:** 2026-06-11
**Status:** Accepted
**Decision:** All runtime code lives under `source/`. Non-runtime artifacts live outside.

---

## Context

We need a clear distinction between runtime code (executable) and non-runtime artifacts (documentation, configs, scripts). Without this rule, the LLM will invent mixed runtime/non-runtime files, breaking deployment pipelines and confusing code structure.

---

## Decision

**All runtime code must live under `source/`.** Everything else (docs, infra, scripts) is non-runtime and should be outside `source/`.

### Structure

```
bornemaps/
├── source/                        ← ALL runtime code
│   ├── shared/                    ← Shared Rust crates
│   │   ├── ev-core/               ← Core domain types, traits
│   │   ├── ev-auth/               ← Authentication helpers
│   │   └── ev-db/                 ← Database access layer
│   ├── services/                  ← Rust microservices
│   │   ├── driver-service/        ← Rust API :8080
│   │   └── admin-service/         ← Rust API :8081
│       └── front/                     ← Mobile and web apps
│       ├── packages/              ← Shared design system, UI kit
│       ├── mobile-driver/         ← Expo app
│       ├── web-driver/            ← React web app
│       └── dashboard/             ← Admin dashboard
├── docs/                          ← Documentation only
├── infra/                         ← Docker, migrations, configs
└── scripts/                       ← Build tools, seed scripts
```

### Rules

1. **Runtime code** → `source/` directory
2. **Documentation** → `docs/` directory
3. **Infrastructure** → `infra/` directory
4. **Scripts** → `scripts/` directory
5. **Never mix runtime and non-runtime in the same directory**

---

## Consequences

### Positive
- Clear deployment targets (all runtime is under `source/`)
- Simpler CI/CD (only scan `source/` for runtime code)
- No confusion about what gets built vs. what's config
- Easier code reviews (separates runtime logic from documentation)

### Negative
- More directories to navigate
- Need to remember file placement rules

---

## Alternatives Considered

### Alternative 1: Mixed in Root
```
bornemaps/
├── mobile-driver/
├── driver-service/
├── docs/
├── infra/
```

**Rejected:** Runtime and non-runtime files mixed, unclear what gets deployed.

### Alternative 2: Flat Structure
```
bornemaps/
├── src/
├── docs/
├── infra/
└── scripts/
```

**Rejected:** `src/` is generic name; `source/` is explicit and self-documenting.

### Alternative 3: Separate Repository
```
runtime-repo/
docs-repo/
infra-repo/
```

**Rejected:** Monorepo provides better coordination between runtime and documentation.

---

## Implementation

- Create `source/` directory
- Move all runtime code into `source/`
- Update CI/CD pipelines to only build `source/`
- Update documentation to reflect new structure

---

## Testing

- Verify all runtime code is under `source/`
- Verify no non-runtime code is under `source/`
- Run deployment pipeline to confirm all components deploy correctly

---

## References

- **Constitution:** Section 7.3 — Source-Rooted Codebase
- **Previous ADR:** ADR-002 — Rust + Actix Services
