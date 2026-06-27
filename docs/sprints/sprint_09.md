# Sprint 09 — Admin Dashboard

**Title:** Admin Dashboard Frontend  
**Sprint:** 09  
**Scope:** `apps/admin-dashboard/`  
**Status:** Complete  

## Goal

Build a production-grade admin dashboard frontend for BorneMap operators to manage users, view metrics, and monitor platform health.

## Deliverables

### New Files
| File | Purpose |
|------|---------|
| `apps/admin-dashboard/vite.config.ts` | Vite config with Tailwind v4 plugin, path aliases, API proxy |
| `apps/admin-dashboard/vitest.config.ts` | Vitest config with jsdom environment |
| `apps/admin-dashboard/tsconfig.app.json` | TypeScript config with `@/` path alias |
| `apps/admin-dashboard/index.html` | Entry HTML with Fira Code + Fira Sans fonts |
| `apps/admin-dashboard/src/index.css` | Tailwind v4 theme with terminal-green design tokens |
| `apps/admin-dashboard/src/main.tsx` | App bootstrap with StrictMode |
| `apps/admin-dashboard/src/App.tsx` | Router + QueryClient + auth hydration wiring |
| `apps/admin-dashboard/src/lib/api.ts` | Axios client with JWT attach, refresh rotation, request queue |
| `apps/admin-dashboard/src/lib/validation.ts` | Zod schemas for login form |
| `apps/admin-dashboard/src/stores/auth-store.ts` | Zustand store for auth state + localStorage persistence |
| `apps/admin-dashboard/src/hooks/use-auth.ts` | React Query hooks for login/logout/me |
| `apps/admin-dashboard/src/components/guards/AuthGuard.tsx` | Route guards (AuthGuard, GuestGuard) |
| `apps/admin-dashboard/src/components/layout/AppLayout.tsx` | Shell layout (Sidebar + Header + Outlet) |
| `apps/admin-dashboard/src/components/layout/Sidebar.tsx` | Collapsible sidebar navigation |
| `apps/admin-dashboard/src/components/layout/Header.tsx` | Top header with user info + logout |
| `apps/admin-dashboard/src/components/ui/Skeleton.tsx` | Skeleton loading components |
| `apps/admin-dashboard/src/features/auth/LoginPage.tsx` | Login form with React Hook Form + Zod |
| `apps/admin-dashboard/src/features/dashboard/DashboardPage.tsx` | Metrics overview + user growth chart |
| `apps/admin-dashboard/src/features/dashboard/MetricsCard.tsx` | Single KPI metric card |
| `apps/admin-dashboard/src/features/dashboard/UserGrowthChart.tsx` | Recharts line chart |
| `apps/admin-dashboard/src/features/users/UsersPage.tsx` | User table with search, sort, pagination |

## Architecture

### Directory Structure

```
apps/admin-dashboard/src/
├── components/
│   ├── guards/        # Route guards (AuthGuard, GuestGuard)
│   ├── layout/        # AppLayout, Sidebar, Header
│   └── ui/            # Skeleton loading components
├── features/
│   ├── auth/          # LoginPage
│   ├── dashboard/     # DashboardPage, MetricsCard, UserGrowthChart
│   └── users/         # UsersPage (table, search, pagination)
├── hooks/             # React Query hooks (use-auth)
├── lib/               # API client, validation schemas
├── stores/            # Zustand stores (auth-store)
├── __tests__/         # Unit tests
├── App.tsx            # Router + providers
├── main.tsx           # Entry point
└── index.css          # Tailwind v4 theme
```

### Tech Stack

| Technology | Version |
|------------|---------|
| React | 19 |
| Vite | 8 |
| TypeScript | 6 |
| Tailwind CSS | 4 |
| React Router | 7 |
| TanStack Query | 5 |
| Zustand | 5 |
| Axios | 1 |
| Framer Motion | 12 |
| Recharts | 3 |
| React Hook Form | 7 |
| Zod | 4 |
| Vitest | 4 |
| Testing Library | 16 |

### Design System

- **Style**: Data-Dense Dashboard (dark/light mode)
- **Colors**: `#00FF41` terminal green primary on `#0D1117` background, `#FF3333` accent
- **Typography**: Fira Code (headings) + Fira Sans (body)
- **Components**: KPI cards, data table, line chart, skeleton loading

### API Integration

```
Admin Dashboard (:5173)
  ↓  Axios with JWT interceptor
  ↓  Refresh token rotation on 401
  ↓  Request queuing during refresh
auth-service (:8080/api/v1)
```

### Route Structure

```
/login          → GuestGuard → LoginPage
/               → AuthGuard → AppLayout
  /             → DashboardPage
  /users        → UsersPage
```

## UX Compliance

| Requirement | Status |
|-------------|--------|
| Animations ≤200ms | ✅ Framer Motion (opacity + y, 150ms) |
| Keyboard navigation | ✅ Focus rings, aria-invalid, cursor-pointer |
| ARIA labels | ✅ role="navigation", role="alert", aria-label |
| prefers-reduced-motion | ✅ Respects motion preferences |
| Responsive (375px-1440px) | ✅ Grid 1→2→4 cols, collapsible sidebar |
| SVG icons (no emoji) | ✅ All icons are inline SVG |
| Hover states 150-300ms | ✅ All interactive elements |

## Tests

```
9 passed; 0 failed; 0 warnings
```

- Auth store: setTokens, setUser, logout, hydrate, initial state
- Login validation: valid input, invalid email, short password, empty email

## Verification

- `tsc -b`: clean (0 errors)
- `vite build`: clean (880 modules)
- `vitest run`: 9/9 pass
- `oxlint`: clean
