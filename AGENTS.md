<!-- SPECKIT START -->
For additional context about technologies to be used, project structure,
shell commands, and other important information, read the current plan
at specs/003-design-system-components/plan.md

## Project Structure

source/           ← ALL runtime code
├── services/      ← Rust microservices
│   ├── shared/    ← Shared Rust crates (ev-core, ev-auth, ev-db)
│   ├── driver-service/ ← Rust/Actix :8080
│   └── admin-service/  ← Rust/Actix :8081
├── front/         ← Mobile and web apps
│   ├── packages/   ← Shared design system, UI kit
│   │   ├── tokens/    ← @bornemap/tokens (Phase 3, completed)
│   │   ├── ui/        ← @bornemap/ui (Phase 3, completed)
│   │   └── scripts/   ← Utility scripts (WCAG validation, bundle analysis)
│   ├── mobile-driver/ ← Expo SDK 54 app (Phase 4)
│   ├── web-driver/    ← React + Leaflet (Phase 4)
│   └── dashboard/     ← React + shadcn/ui (Phase 4)
├── pnpm-workspace.yaml ← Workspace configuration
├── package.json ← Root scripts (typecheck, lint, test)
├── tsconfig.base.json ← Shared TypeScript config
└── .eslintrc.cjs ← ESLint configuration
└── .prettierrc ← Prettier configuration

## Phase Status

- **MVP-1 Phase 1**: Complete (backend services, DB)
- **MVP-1 Phase 2**: Complete (backend services validation)
- **MVP-1 Phase 3**: Complete (design system packages)
  - @bornemap/tokens with 9 token categories
  - @bornemap/ui with ThemeProvider + 7 components
  - All WCAG AA contrast checks passing
  - TypeScript strict mode typechecking passing
  - Bundle size < 50KB gzipped
- **MVP-1 Phase 4**: Pending (mobile driver app, web driver app)

## Design System Build Commands

```bash
cd source/front

# Build both packages
pnpm build

# Build tokens only
pnpm build:tokens

# Build UI only
pnpm build:ui

# Typecheck all packages
pnpm typecheck

# Lint all packages
pnpm lint

# Run tests
pnpm test

# Validate WCAG AA contrast
pnpm --filter @bornemap/ui validate-contrast

# Analyze bundle size
pnpm --filter @bornemap/ui analyze-bundle
```

## Design System Artifacts

- Token reference: `design-system/bornemap/MASTER.md`
- Research: `specs/003-design-system-components/research.md`
- Data model: `specs/003-design-system-components/data-model.md`
- Contracts: `specs/003-design-system-components/contracts/package-apis.md`
- Quickstart: `specs/003-design-system-components/quickstart.md`
<!-- SPECKIT END -->
