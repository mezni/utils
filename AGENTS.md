<!-- SPECKIT START -->
For additional context about technologies to be used, project structure,
shell commands, and other important information, read the current plan
<!-- SPECKIT END -->

## Active Feature: Driver Web App with Mock Data

**Plan**: [plan.md](specs/002-driver-web-mock/plan.md)

**Spec**: [spec.md](specs/002-driver-web-mock/spec.md)

**Status**: Planning complete, ready for implementation tasks

---

### Key Deliverables

1. **App Scaffold** (`apps/driver-web/`)
   - Vite + React + TypeScript with Tailwind + i18n
   - Routes for 6 screens, RTL support for Arabic

2. **Mock Data** (`apps/driver-web/src/mocks/`)
   - 15 stations with Tunisian coordinates
   - 2–4 chargers per station (Type 2, CCS, CHAdeMO)
   - 3–5 reviews per station (Arabic and French)

3. **Driver-Specific Components** (`apps/driver-web/src/components/`)
   - 9 components: MobileTopBar, SearchBar, FilterPills, MapPinMarker, ZoomControls, StationCard, ChargerRow, ReviewCard, BottomStationCard
   - TypeScript prop interfaces
   - Unit tests for all variants and states

4. **Screens** (`apps/driver-web/src/screens/`)
   - Home/Map, Station Detail, Search Results, Favorites, Profile, Login/Register

---

### Technical Approach

- **Vite 5** + **React 18** + **TypeScript 5.x** for the SPA
- **React Router v6** with createBrowserRouter for navigation
- **Tailwind CSS** extending `packages/ui/tailwind.config.base.js` for design tokens
- **react-i18next** for Arabic/French i18n with automatic RTL switching
- **Vitest** + **@testing-library/react** for testing
- **Sidebar + Top Bar** layout pattern (persistent TopBar, sidebar as nav panel)
- **No backend calls** — all data from local mock TypeScript files

---

### Design Principles

- All visual values from tokens (no hardcoding) — via `packages/ui`
- WCAG 2.1 AA accessibility compliance on all screens
- Arabic RTL works correctly on every screen
- Mock data is placeholder — replaceable with real API in Phase 5

---

### Project Structure

```
apps/driver-web/
├── src/
│   ├── components/
│   │   ├── MobileTopBar.tsx
│   │   ├── SearchBar.tsx
│   │   ├── FilterPills.tsx
│   │   ├── MapPinMarker.tsx
│   │   ├── ZoomControls.tsx
│   │   ├── StationCard.tsx
│   │   ├── ChargerRow.tsx
│   │   ├── ReviewCard.tsx
│   │   └── BottomStationCard.tsx
│   ├── screens/
│   │   ├── HomeMapScreen.tsx
│   │   ├── StationDetailScreen.tsx
│   │   ├── SearchResultsScreen.tsx
│   │   ├── FavoritesScreen.tsx
│   │   ├── ProfileScreen.tsx
│   │   └── LoginRegisterScreen.tsx
│   ├── mocks/
│   │   ├── stations.ts
│   │   ├── chargers.ts
│   │   ├── reviews.ts
│   │   └── users.ts
│   ├── i18n/
│   │   ├── ar.json
│   │   ├── fr.json
│   │   └── index.ts
│   ├── hooks/
│   ├── types/
│   │   └── index.ts
│   ├── App.tsx
│   ├── main.tsx
│   └── index.css
├── package.json
├── tsconfig.json
├── vite.config.ts
├── tailwind.config.ts
└── postcss.config.js
```

---

### Success Criteria

- ✅ All 6 screens render with realistic mock data when navigated to
- ✅ Navigation between all screens works (click, back, direct URL)
- ✅ Arabic RTL layout is correct on every screen
- ✅ French layout renders correctly with translated strings
- ✅ No backend calls made (verified via network tab)
- ✅ All 9 driver-specific components render with required props and states
- ✅ `pnpm build` passes with zero warnings
- ✅ All static strings translated in ar.json and fr.json
