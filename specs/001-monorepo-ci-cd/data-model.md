# Data Model: Monorepo and CI/CD Setup

## Overview

This sprint defines the monorepo structure and shared abstractions. No runtime business entities are created — only build-time and infrastructure abstractions.

## Entities

### Rust Workspace
| Attribute | Type | Description |
|---|---|---|
| root | `Cargo.toml` | Defines workspace members and shared dependency versions |
| members | String[] | `services/driver-service`, `services/admin-service`, `crates/ev-core`, `crates/ev-db` |
| shared deps | Table | `[workspace.dependencies]` — actix-web, sqlx, tokio, serde, etc. |

### npm Workspace
| Attribute | Type | Description |
|---|---|---|
| root | `package.json` | Defines `"workspaces"` field and root scripts |
| workspaces | String[] | `apps/*`, `packages/*` |
| scripts | Object | `dev:*`, `build:*`, `lint`, `test` |

### CI Workflow
| Attribute | Type | Description |
|---|---|---|
| file | `.yml` | GitHub Actions workflow in `.github/workflows/` |
| trigger | Path filter | `on.push.paths` scoped to specific directories |
| jobs | Job[] | Lint, test, build steps specific to the scoped component |
| caching | Boolean | `actions/cache` for `~/.npm` |

### Shared Crate (ev-core)
| Attribute | Type | Description |
|---|---|---|
| id generators | Function[] | `new_usr()`, `new_prt()`, `new_stn()`, `new_chg()`, `new_rev()`, `new_evt()` |
| types | Enum[] | `ConnectorType`, `ChargerStatus`, `AvailabilityStatus` |

### Shared Crate (ev-db)
| Attribute | Type | Description |
|---|---|---|
| pool factory | Function | `create_pool(database_url)` |
| pagination | Struct | `OffsetParams` (limit, offset), `PaginatedResponse<T>` (data, total, limit, offset) |

## Relationships

```
RustWorkspace 1──N Crate (ev-core, ev-db)
RustWorkspace 1──N Service (driver-service, admin-service)
npmWorkspace 1──N App (driver-web, driver-mobile, dashboard)
npmWorkspace 1──N Package (ui, api-client-*)
CIWorkflow 0..1──N Service/App (path-scoped trigger)
```

## Validation Rules

- Every Rust service crate must be a workspace member
- Every JS/TS app/package must be declared in `package.json#workspaces`
- Cargo dependency versions must be centralized in `[workspace.dependencies]`
- CI workflows must only trigger on their scoped paths
- No service may depend on another service crate (only on shared crates)
