# File Layout Conventions

This document defines the canonical file layout and naming conventions
for the BorneMap monorepo. All new files MUST conform to these rules.

## Directory Structure

```
bornemap-monorepo/
├── Cargo.toml                    # Root virtual manifest (workspace)
├── docker-compose.dev.yml        # Local dev services
├── .gitignore
├── .github/workflows/            # CI pipeline definitions
├── sources/
│   ├── backend/                  # Rust backend (single binary)
│   │   ├── Cargo.toml
│   │   ├── Dockerfile.dev
│   │   ├── migrations/           # SQLx migrations (*.up.sql, *.down.sql)
│   │   ├── src/
│   │   │   ├── main.rs           # Actix-web entry point
│   │   │   ├── domain/           # Domain modules (clean architecture)
│   │   │   │   ├── users/
│   │   │   │   ├── partners/
│   │   │   │   ├── stations/
│   │   │   │   ├── chargers/
│   │   │   │   ├── connector_types/
│   │   │   │   └── infrastructure/  # Shared queries (e.g., nearby)
│   │   │   └── utils/
│   │   │       └── id_generator.rs
│   │   └── sqlx-data.json
│   └── frontend/
│       ├── package.json           # pnpm workspace root
│       ├── pnpm-workspace.yaml
│       ├── packages/
│       │   └── ui/                # Shared design system
│       │       ├── tailwind.config.ts
│       │       └── src/components/ui/
│       └── apps/
│           ├── admin-portal/      # React + Vite + TS
│           ├── partner-dashboard/ # React + Vite + TS
│           └── mobile-driver/     # Expo SDK 51 + React Native
```

## Naming Conventions

| Artifact | Convention | Example |
|----------|-----------|---------|
| Rust files | `snake_case.rs` | `id_generator.rs` |
| TypeScript files | `kebab-case.tsx` / `kebab-case.ts` | `scrollable-table.tsx` |
| SQL migrations | `YYYYMMDDHHMMSS_description.up.sql` | `20260525000000_init.up.sql` |
| Docker images | `kebab-case` | `bornemap-api` |
| CI workflows | `kebab-case.yml` | `backend.yml` |
| Git branches | `NNN-short-feature-name` | `001-project-scaffolding-cicd` |
| Domain modules | `snake_case/` with `mod.rs` | `domain/station_connector_types/` |

## API Route Pattern

All API endpoints MUST be prefixed with `/api/v1/`:

```
/api/v1/{resource}
/api/v1/{resource}/{id}
/api/v1/{resource}/{id}/{sub-resource}
```

## Design Token Contract

All visual tokens are defined in `packages/ui/tailwind.config.ts`.
View files MUST reference these tokens by name:

```tsx
// ✅ Correct — uses token
<div className="bg-accent text-surface" />

// ❌ Wrong — hardcoded hex
<div className="bg-[#22c55e] text-[#ffffff]" />
```

## CI Workflow Contract

Three workflow files under `.github/workflows/`, each triggered by
path-based events matching their relevant source tree.
