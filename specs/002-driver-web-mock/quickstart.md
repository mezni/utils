# Quickstart: Driver Web App

## Prerequisites

- Node.js 20+
- pnpm 10+
- Monorepo initialized with `pnpm install` at root level
- `packages/ui` built (Sprint 1.1)

## Setup

```bash
# From repo root, install dependencies
pnpm install

# Build the UI package (required dependency)
pnpm --filter @borne-map/ui build
```

## Development

```bash
# Start dev server with HMR
pnpm --filter @borne-map/driver-web dev
```

Opens at `http://localhost:5173` by default.

## Build

```bash
# Production build
pnpm --filter @borne-map/driver-web build

# Output in apps/driver-web/dist/
```

## Test

```bash
# Run unit and component tests
pnpm --filter @borne-map/driver-web test

# Watch mode
pnpm --filter @borne-map/driver-web test --watch
```

## Lint & Format

```bash
# Lint
pnpm --filter @borne-map/driver-web lint

# Format check
pnpm --filter @borne-map/driver-web format
```

## Project Scripts

| Script | Command | Description |
|--------|---------|-------------|
| `dev` | `vite` | Start development server |
| `build` | `tsc && vite build` | Type-check and build for production |
| `preview` | `vite preview` | Preview production build locally |
| `test` | `vitest run` | Run tests once |
| `test:watch` | `vitest` | Run tests in watch mode |
| `lint` | `eslint src/` | Lint source code |
| `format` | `prettier --check src/` | Check formatting |

## i18n

- Default language: French
- Language switching via settings (future: auto-detect via `i18next-browser-languagedetector`)
- Arabic selects `dir="rtl"` on `<html>`
- Translation files in `src/i18n/`

## Package Name

The app package should be named `@borne-map/driver-web` in `apps/driver-web/package.json`.
