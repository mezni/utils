# Research: Project Scaffolding & CI/CD

**Phase**: 0 (Outline & Research)

## Decisions

### Decision 1: Monorepo structure under `sources/`

- **Decision**: All project code lives under `sources/backend/` and `sources/frontend/`
- **Rationale**: Constitution Principle I mandates modular monorepo layout.
  Separating backend (Rust) from frontend (JS/TS) avoids cross-language
  build complexity while keeping a single repository.
- **Alternatives considered**: Flat root layout rejected — would mix Rust
  and JS tooling at the root, creating confusion.

### Decision 2: pnpm as frontend package manager

- **Decision**: Use pnpm workspaces for frontend monorepo
- **Rationale**: pnpm provides strict dependency isolation per package,
  faster installs via content-addressable store, and built-in workspace
  protocol for local package references. The constitution specifies
  `pnpm install --frozen-lockfile` in CI.
- **Alternatives considered**: npm workspaces (slower, no strict
  isolation), Yarn workspaces (additional tooling complexity).

### Decision 3: Rust workspace virtual manifest

- **Decision**: Root `Cargo.toml` is a virtual workspace, actual crate
  config is in `sources/backend/Cargo.toml`
- **Rationale**: Keeps Rust tooling (`cargo test`, `cargo clippy`)
  isolated to the backend directory. A root virtual manifest allows
  future crate additions (e.g., shared libraries) without restructuring.
- **Alternatives considered**: Single crate at root — would pollute root
  directory mixing Rust and Node.js config files.

### Decision 4: PostgreSQL service container in CI

- **Decision**: Backend CI provisions a `postgis/postgis:16-3.4-alpine`
  service container for SQLx compile-time verification
- **Rationale**: SQLx requires a live database at compile time to verify
  queries. An alternative (committing `sqlx-data.json` with
  `SQLX_OFFLINE=true`) avoids the DB dependency but is less strict.
  The service container approach catches more errors.
- **Alternatives considered**: SQLX_OFFLINE with committed data.json —
  deferred to post-MVP0 once the data.json is stable.

### Decision 5: Path-based CI triggers

- **Decision**: Backend CI triggers only on `sources/backend/**` and
  `Cargo.toml` changes. Frontend CI triggers only on `sources/frontend/**`
  changes. Docker smoke test triggers on Dockerfile or compose changes.
- **Rationale**: Saves CI minutes by not running irrelevant checks.
  Monorepo makes this essential for developer productivity.
- **Alternatives considered**: Run everything on every change —
  wasteful for a monorepo with separate language ecosystems.

### Decision 6: Expo SDK 51 managed workflow

- **Decision**: Mobile app uses Expo SDK 51 with exact pinned versions
- **Rationale**: Constitution Principle IV mandates managed Expo Go with
  locked dependencies. SDK 51 is the latest stable at project start.
- **Alternatives considered**: Expo SDK 52 (too new, ecosystem stability
  risk), bare React Native (banned by constitution).

### Decision 7: Tailwind CSS as design system foundation

- **Decision**: Shared `packages/ui/tailwind.config.ts` defines all
  design tokens (colors, radii, spacing, shadows)
- **Rationale**: Constitution Principle III requires centralized design
  tokens from a single `tailwind.config.ts`. Hex codes in view files
  are banned.
- **Alternatives considered**: CSS custom properties (loses Tailwind
  utility integration), Sass variables (no type-safe config).

### Decision 8: Cargo.toml workspace members

- **Decision**: Define `sources/backend` as the sole Cargo workspace member
- **Rationale**: Single backend binary for MVP0. Future microservice
  extraction adds new members to the workspace.
- **Alternatives considered**: Multiple workspace members from the start
  — premature optimization. Constitution Principle V says modularize
  interfaces, not modules.
