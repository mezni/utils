<!-- SPECKIT START -->
For additional context about technologies to be used, project structure,
shell commands, and other important information, read the current plan
at specs/003-driver-mobile-mock/plan.md
<!-- SPECKIT END -->

## Active Feature: Driver Mobile App with Mock Data

**Plan**: [plan.md](specs/003-driver-mobile-mock/plan.md)

**Spec**: [spec.md](specs/003-driver-mobile-mock/spec.md)

**Status**: Plan complete — ready for implementation tasks

---

### Key Deliverables

1. **App Scaffold** (`apps/driver-mobile/`)
   - Expo + React Native + TypeScript with native tokens + i18n
   - Bottom tab navigator (5 tabs) + stack navigator (2 stack screens)
   - RTL support for Arabic via `I18nManager.forceRTL()`

2. **Mock Data** (`apps/driver-mobile/src/mocks/`)
   - 15 stations with Tunisian coordinates (same shape as web)
   - 2–4 chargers per station (Type2, CCS, CHAdeMO)
   - 3–5 reviews per station (Arabic and French)

3. **Mobile-Specific Components** (`apps/driver-mobile/src/components/`)
   - 12 components: MobileTopBar, SearchBar, FilterPills, MapPinMarker, ZoomControls, StationCard, ChargerRow, ReviewCard, BottomStationCard, SpecRow, CenterActionButton, BottomTabBar
   - TypeScript prop interfaces

4. **Screens** (`apps/driver-mobile/src/screens/`)
   - Map/Home, Station List, Station Detail, Search, Favorites, Profile, Login/Register

---

### Technical Approach

- **Expo SDK 52** + **React Native 0.76** + **TypeScript 5.x** for mobile
- **React Navigation v6** with bottom tabs + native stack
- **Native tokens** from `packages/ui/src/tokens/native.ts` for all visual values
- **react-i18next** + **expo-localization** for Arabic/French i18n with RTL
- **react-native-safe-area-context** for safe area insets
- **No backend calls** — all data from local mock TypeScript files

---

### Design Principles

- All visual values from tokens (no hardcoding) — via `packages/ui/src/tokens/native.ts`
- Arabic RTL works correctly on every screen via `I18nManager.forceRTL()`
- Mock data is placeholder — replaceable with real API in Phase 5
- iOS and Android parity from single Expo managed codebase

---

### Project Structure

```
apps/driver-mobile/
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
│   │   ├── BottomStationCard.tsx
│   │   ├── SpecRow.tsx
│   │   ├── CenterActionButton.tsx
│   │   └── BottomTabBar.tsx
│   ├── screens/
│   │   ├── HomeMapScreen.tsx
│   │   ├── StationListScreen.tsx
│   │   ├── StationDetailScreen.tsx
│   │   ├── SearchScreen.tsx
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
│   │   ├── useStations.ts
│   │   ├── useFavorites.ts
│   │   └── useMockFilter.ts
│   ├── navigation/
│   │   ├── RootNavigator.tsx
│   │   └── types.ts
│   ├── types/
│   │   └── index.ts
│   ├── App.tsx
│   └── index.css
├── app.json
├── babel.config.js
├── tsconfig.json
├── package.json
└── metro.config.js
```

---

### Success Criteria

- ✅ All 7 screens render with realistic mock data on iOS simulator and Android emulator
- ✅ Navigation between all screens works via bottom tabs and stack (forward and back)
- ✅ Arabic RTL layout is correct on every screen
- ✅ French layout renders correctly with translated strings on all screens
- ✅ No backend calls made (verified via network inspector)
- ✅ All 12 mobile components render with required props and all visual states
- ✅ `pnpm build` passes for `apps/driver-mobile` with zero warnings
