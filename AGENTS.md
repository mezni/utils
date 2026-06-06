<!-- SPECKIT START -->
For additional context about technologies to be used, project structure,
shell commands, and other important information, read the current plan
at specs/004-dashboard-mock/plan.md
<!-- SPECKIT END -->

## Active Feature: Dashboard App with Mock Data

**Plan**: [plan.md](specs/004-dashboard-mock/plan.md)

**Spec**: [spec.md](specs/004-dashboard-mock/spec.md)

**Status**: Plan complete — ready for implementation tasks

---

### Key Deliverables

1. **App Scaffold** (`apps/dashboard/`)
   - Vite + React 19 + TypeScript with design tokens + i18n
   - React Router v7 with role-based navigation
   - RTL support for Arabic via CSS logical properties

2. **Mock Data** (`apps/dashboard/src/mocks/`)
   - 5 partners, 10 users, 7-10 report stat cards
   - 15 stations, 50+ chargers, 60+ reviews (reused from driver apps)

3. **Dashboard-Specific Components** (`apps/dashboard/src/components/`)
   - 6 components: AppShell (Sidebar, TopBar), PageContent, DataCard, DataTable, StatCard, NavigationItem
   - TypeScript prop interfaces

4. **Screens** (`apps/dashboard/src/screens/`)
   - Partner: Overview, My Stations, Station Edit, Charger Management, Availability Update, Reports (6 screens)
   - Admin: Overview, Users, Partners, Stations, Chargers, Reviews, Reports (7 screens)

---

### Technical Approach

- **Vite 6** + **React 19** + **TypeScript 5.7** for web
- **React Router v7** with role-based routing
- **Design tokens** from `packages/ui` for all visual values
- **react-i18next** for Arabic/French i18n with RTL
- **React Context** for role management (mock auth)
- **No backend calls** — all data from local mock TypeScript files

---

### Design Principles

- All visual values from tokens (no hardcoding) — via `packages/ui`
- Arabic RTL works correctly on every screen via CSS logical properties
- Mock data is placeholder — replaceable with real API in Phase 5
- Role-based UI (Partner vs Admin) via React Context
- Single-page application with client-side routing

---

### Project Structure

```
apps/dashboard/
├── src/
│   ├── components/
│   │   ├── AppShell/
│   │   │   ├── AppShell.tsx
│   │   │   ├── Sidebar/
│   │   │   │   ├── Sidebar.tsx
│   │   │   │   ├── BrandHeader.tsx
│   │   │   │   ├── NavigationItem.tsx
│   │   │   │   └── BottomActions.tsx
│   │   │   └── TopBar.tsx
│   │   ├── PageContent/
│   │   │   └── PageContent.tsx
│   │   ├── DataCard/
│   │   │   └── DataCard.tsx
│   │   ├── DataTable/
│   │   │   ├── DataTable.tsx
│   │   │   └── table.types.ts
│   │   └── StatCard/
│   │   │   └── StatCard.tsx
│   ├── screens/
│   │   ├── OverviewScreen.tsx
│   │   ├── MyStationsScreen.tsx
│   │   ├── StationEditScreen.tsx
│   │   ├── ChargerManagementScreen.tsx
│   │   ├── AvailabilityUpdateScreen.tsx
│   │   ├── ReportsScreen.tsx
│   │   ├── UsersScreen.tsx
│   │   ├── PartnersScreen.tsx
│   │   ├── StationsScreen.tsx
│   │   ├── ChargersScreen.tsx
│   │   └── ReviewsScreen.tsx
│   ├── mocks/
│   │   ├── partners.ts
│   │   ├── stations.ts
│   │   ├── chargers.ts
│   │   ├── users.ts
│   │   ├── reviews.ts
│   │   └── reports.ts
│   ├── i18n/
│   │   ├── ar.json
│   │   ├── fr.json
│   │   └── index.ts
│   ├── hooks/
│   │   ├── useRole.ts
│   │   ├── useMockData.ts
│   │   └── useNavigation.ts
│   ├── context/
│   │   └── RoleContext.tsx
│   ├── types/
│   │   └── index.ts
│   ├── App.tsx
│   └── index.css
├── package.json
├── tsconfig.json
├── vite.config.ts
└── tailwind.config.js
```

---

### Success Criteria

- ✅ All 13 screens render with realistic mock data in browser
- ✅ Navigation between all screens works via sidebar (Partner: 6 screens, Admin: 7 screens)
- ✅ Arabic RTL layout is correct on every screen (sidebar aligns right, tables formatted correctly)
- ✅ French layout renders correctly with translated strings on all screens
- ✅ Role switching (Partner ↔ Admin) completes within 1 second and updates UI correctly
- ✅ No backend calls made (verified via network inspector)
- ✅ All 6 dashboard components render with required props and all visual states
- ✅ `pnpm build` passes for `apps/dashboard` with zero warnings
- ✅ All visual values consumed from `packages/ui` design tokens (zero hardcoded values)
