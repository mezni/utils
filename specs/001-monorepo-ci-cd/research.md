# Research: Monorepo and CI/CD Setup

## Phase 0 — Unknown Resolution

All technical unknowns were resolved from the project constitution and spec. No NEEDS CLARIFICATION markers existed.

### Technology Choices

| Decision | Value | Rationale |
|---|---|---|
| Rust workspace version | 1.95 | Specified by user |
| Node.js version | 20.20 | Specified by user |
| npm version | 10.8 | Specified by user (npm workspaces) |
| CI platform | GitHub Actions | Per constitution |
| CI workflow count | 6 | 1 full workspace + 5 path-scoped (2 Rust + 3 frontend) |
| Docker Compose split | dev + prod | Clarified during spec review |
| npm caching | actions/cache for ~/.npm | Clarified during spec review |
| PostgreSQL version | 16 + PostGIS 3.4 | Per constitution |
| Actix-web version | 4 | Per constitution |
| sqlx version | 0.8 | Per constitution |

### Alternatives Considered

| Alternative | Rejected Because |
|---|---|
| Single CI workflow with matrix | Reduces parallelism, harder to debug path-specific failures |
| pnpm workspaces | User specified npm 10.8 |
| Single Docker Compose file | Violates dev/prod separation principle from constitution |
