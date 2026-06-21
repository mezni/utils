# BorneMap — Engineering Standards
**Version:** 1.0
**Date:** June 2026
**Status:** Active

---

## 1. Rust Standards

### Crate Structure
```
<service>/
├── api/        # Actix-web handlers, routes, DTOs
├── domain/    # Pure domain logic, no framework deps
├── application/  # Use-case orchestration
└── infrastructure/  # DB, HTTP, cache adapters
```

### Rules
- `domain/` = zero external dependencies. Pure Rust + stdlib only.
- `api/` may depend on `application/`, `domain/`, and `infrastructure/`
- `application/` may depend on `domain/` and `infrastructure/` traits
- `infrastructure/` may depend on `domain/` traits
- No circular crate dependencies

### SQLx
- All queries must be compile-time checked (`query!`, `query_as!`, etc.)
- No raw SQL string construction
- Migrations in `infrastructure/migrations/`

## 2. TypeScript Standards

### Package Structure
```
<packages/> or <apps/>
├── src/
│   ├── components/   (if applicable)
│   ├── hooks/        (if applicable)
│   ├── types/
│   └── utils/
├── index.ts
└── package.json
```

### Rules
- Strict TypeScript mode
- No `any` — use `unknown` + type guards
- Barrel exports from `index.ts` only
- No UI logic in API client packages
- No API calls in UI component packages

## 3. Testing Standards

### Coverage Thresholds
| Layer | Minimum |
|---|---|
| `domain/` (Rust) | 100% |
| `api/` (Rust) | 90% |
| Integration (Rust) | Required for every endpoint |
| TypeScript packages | 80% |

### Rules
- No mocks for database logic — use test DB instances
- Regression tests required for every fixed bug
- API tests must validate response schema matches OpenAPI spec

## 4. Documentation Standards

### Required Documents
| Document | Location | Updated |
|---|---|---|
| Architecture | `docs/architecture.md` | Per sprint |
| API Contracts | `platform/api/openapi/*.yaml` | Per endpoint |
| System State | `docs/SYSTEM_STATE.md` | End of sprint |
| Roadmap Status | `docs/roadmap_status.md` | End of sprint |

### Doc Drift
- Code overrides docs. When code diverges from docs, docs must be updated immediately.
- SpecKit validates doc drift on every SPEC phase.

## 5. Git Standards

### Branch Naming
```
<type>/<sprint-id>-<short-description>
```
Types: `feat`, `fix`, `chore`, `docs`

### Commit Messages
```
<sprint-id>: <short description>
```

### Rules
- No direct pushes to `main`
- PRs require passing CI + SpecKit validation
- PRs require at least one review
