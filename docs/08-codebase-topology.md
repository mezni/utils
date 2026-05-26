# BorneMap — Microservice-Ready Codebase Topology

## Monorepo Structure

```
bornemap-monorepo/
├── Cargo.toml                  # Root Cargo virtual manifest workspace configuration
├── docker-compose.dev.yml      # Local container cluster definitions
├── .github/
│   └── workflows/
│       ├── backend.yml         # Rust CI: fmt, clippy, test, build
│       ├── frontend.yml        # Frontend CI: lint, type-check, build
│       └── docker.yml          # Docker Compose smoke test
├── sources/
│   ├── backend/                # Server Engine Architecture
│   │   ├── Cargo.toml
│   │   ├── Dockerfile.dev      # Backend microservice runtime container context
│   │   ├── migrations/         # SQLx standalone database migration scripts
│   │   │   ├── 20260525000000_init.up.sql
│   │   │   └── 20260525000001_seed_sandbox.up.sql
│   │   ├── src/
│   │   │   ├── main.rs         # Actix-web initialization mapping /api/v1/ scope
│   │   │   ├── domain/         # Clean domain split
│   │   │   │   ├── mod.rs
│   │   │   │   └── infrastructure/
│   │   │   │       ├── mod.rs
│   │   │   │       └── repository.rs
│   │   │   └── utils/
│   │   │       └── id_generator.rs
│   │   └── sqlx-data.json
│   │
│   ├── frontend/               # Complete Client Portals Ecosystem Topology
│   │   ├── README.md
│   │   ├── packages/
│   │   │   └── ui/             # Shared Design System Tokens
│   │   │       ├── src/components/ui/scrollable-table.tsx
│   │   │       └── tailwind.config.ts
│   │   └── apps/
│   │       ├── admin-portal/   # Web Administration Portal App
│   │       ├── partner-dashboard/ # Multi-tenant Dashboard App
│   │       └── mobile-driver/  # Managed Expo Driver Canvas App
```

## Workspace Boundaries

### Backend (`sources/backend/`)

Rust workspace crate compiled to a single binary. Domain modules under `src/domain/` are structured for future extraction into standalone microservices:

- **`domain/infrastructure/`** — Route handlers (`mod.rs`) and data access layer (`repository.rs`)
- **`utils/`** — Cross-domain utilities like `id_generator.rs`

The `sqlx-data.json` file supports offline SQLx compile-time verification without requiring a live database connection during CI builds.

### Frontend (`sources/frontend/`)

Monorepo with shared packages and multiple application targets:

| Path | Purpose |
|------|---------|
| `packages/ui/` | Shared design system: Tailwind config tokens, reusable components (`ScrollableTable`, etc.) |
| `apps/admin-portal/` | Web Administration Portal (React) |
| `apps/partner-dashboard/` | Multi-tenant Partner Dashboard (React) |
| `apps/mobile-driver/` | Managed Expo Driver App (React Native) |

### Design System Sharing

The `packages/ui/` directory is the single source of truth for:

- Tailwind configuration tokens (colors, spacing, radii, shadows)
- Shared UI components used across all frontend apps
- Any shared utilities or hooks

Each app under `apps/` imports from `packages/ui/` to maintain visual consistency.

## CI/CD Pipeline (`.github/workflows/`)

GitHub Actions workflows enforce code quality gates on every push and
pull request. Each workflow uses path-based triggers to run only when
its relevant source paths change.

| Workflow | Triggers On | Steps |
|----------|------------|-------|
| `backend.yml` | `sources/backend/**`, `Cargo.toml` | `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test`, `cargo build --release` |
| `frontend.yml` | `sources/frontend/**` | `pnpm install --frozen-lockfile`, `pnpm -r lint`, `pnpm -r type-check`, `pnpm -r build` |
| `docker.yml` | `docker-compose.dev.yml`, `Dockerfile.dev` | `docker compose up -d --wait`, health check curl, `docker compose down -v` |

The backend workflow provisions a PostgreSQL + PostGIS service container
for SQLx compile-time verification during `cargo test` and `cargo clippy`.

CI is continuous integration only — no auto-deploy (CD) at this stage.
