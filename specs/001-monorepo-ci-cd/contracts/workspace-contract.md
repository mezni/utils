# Workspace Contract

## Rust (Cargo Workspace)

- Workspace root: `/Cargo.toml`
- Members: `services/driver-service`, `services/admin-service`, `crates/ev-core`, `crates/ev-db`
- Shared deps in `[workspace.dependencies]`: actix-web 4, sqlx 0.8, tokio 1, serde 1, serde_json 1, tracing 0.1, tracing-subscriber 0.3, nanoid 0.4, dotenvy 0.15, thiserror 1, chrono 0.4, uuid 1
- No service crate may depend on another service crate
- All shared crates have path dependencies

## JavaScript/TypeScript (npm Workspace)

- Workspace root: `/package.json` with `"workspaces": ["apps/*", "packages/*"]`
- Root scripts: `dev:driver-web`, `dev:dashboard`, `dev:mobile`, `build:driver-web`, `build:dashboard`, `lint`, `test`
- TypeScript base config: `/tsconfig.base.json`
- ESLint base config: `/.eslintrc.base.js`
- Prettier config: `/.prettierrc`

## Dependency Rules

- `packages/api-client-driver` → used by `apps/driver-web`, `apps/driver-mobile` only
- `packages/api-client-admin` → used by `apps/dashboard` only
- `packages/ui` → used by `apps/driver-web`, `apps/dashboard` only
- `packages/ui/native` → used by `apps/driver-mobile` only
- `packages/auth-client` → used by `apps/driver-web`, `apps/dashboard` only
- No app may import tokens directly from another app
