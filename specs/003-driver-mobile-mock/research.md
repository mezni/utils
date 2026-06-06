# Research: Driver Mobile App with Mock Data

**Phase 0 — Technical Decision Records**

## 1. Framework: Expo vs React Native CLI

**Decision**: Expo managed workflow (SDK 52+)

| Factor | Expo (managed) | React Native CLI |
|--------|---------------|------------------|
| Setup time | Minutes (`npx create-expo-app`) | Hours (Xcode, Android Studio, SDK config) |
| Build pipeline | EAS Build / Expo Go | Manual native config |
| Font loading | `expo-font` — declarative | Manual plist/asset config |
| RTL | `I18nManager` built-in | Same (identical API) |
| i18n | `expo-localization` for locale detection | Manual native module |
| Safe area | `react-native-safe-area-context` (expo install) | Same package but manual linking |
| CI/CD complexity | Lower | Higher (native build tooling) |

**Rationale**: The spec targets both iOS and Android from a single codebase, and the implementation plan explicitly says "Expo + React Native + TypeScript". Expo managed workflow is the fastest path to iOS/Android parity with zero native configuration. font loading, localization detection, and safe area handling are simpler.

## 2. Navigation: React Navigation v6

**Decision**: React Navigation 6 with bottom tabs + native stack

- `@react-navigation/native` — navigation container
- `@react-navigation/bottom-tabs` — 5-tab navigator (Map, Stations List, Search, Favorites, Profile)
- `@react-navigation/native-stack` — stack navigator for Station Detail and Login/Register screens

**Rationale**: Industry standard for Expo/RN navigation. Bottom tabs match the spec requirement. Native stack gives platform-native transitions.

## 3. RTL Implementation

**Decision**: `I18nManager.forceRTL()` on language change

```tsx
useEffect(() => {
  const isRTL = i18n.language === 'ar'
  if (I18nManager.isRTL !== isRTL) {
    I18nManager.forceRTL(isRTL)
    // Requires app reload for full I18nManager effect
    // Updates.restartAsync() from expo-updates
  }
}, [i18n.language])
```

**Note**: `I18nManager.forceRTL()` requires an app restart to take full effect (React Native limitation). The text direction and alignment will change immediately via i18next, but the layout direction shift (e.g., flexDirection for all components) requires a reload. This is the documented React Native behavior.

## 4. i18n: react-i18next + i18next + expo-localization

**Decision**: Same stack as Sprint 1.2 web app, with `expo-localization` for device locale detection

- i18next (core)
- react-i18next (React hooks: `useTranslation`, `Trans`)
- expo-localization (detect device locale)
- Translation files: `ar.json`, `fr.json` — reuse web translations, extend with mobile-specific keys

**Reuse strategy**: Copy `src/i18n/ar.json` and `src/i18n/fr.json` from `apps/driver-web` as base, then add mobile-specific keys (tab labels, bottom sheet labels, etc.).

## 5. Safe Area Handling

**Decision**: `react-native-safe-area-context` with `SafeAreaView`

- MobileTopBar uses `useSafeAreaInsets().top` for top padding
- BottomTabBar uses `useSafeAreaInsets().bottom` for bottom padding
- Screens wrapped in `SafeAreaView` for general safe area

## 6. Font Loading

**Decision**: `expo-font` + `@expo-google-fonts/plus-jakarta-sans`

Plus Jakarta Sans is the constitution-mandated font family. Loaded via `expo-font` at app startup in a splash screen hook.

## 7. Testing Strategy

**Decision**: Jest + @testing-library/react-native (standard RN testing stack)

**Note**: Testing is OPTIONAL per constitution unless explicitly requested in the spec. The spec does not mention testing. However, following Sprint 1.2 precedent, basic component tests are beneficial.

## 8. State Management

**Decision**: React hooks + React Context

No external state library (Redux, Zustand, etc.) needed. Mock data is loaded once from TypeScript files. Favorites state managed via React Context + state (no persistence required per spec). The web app used `useStations`, `useFavorites`, `useMockFilter` hooks — same pattern for mobile.

## 9. Map Placeholder Approach

**Decision**: Full-bleed `View` with `backgroundColor: brandLight (#EAF0E6)` as map background. Station pins are absolutely positioned `View` components with circular shape and `shadow.pin` from tokens. No real map library (Google Maps, Mapbox, Leaflet) is used in this sprint.

Pin positions derived proportionally from station coordinates mapped to the container dimensions.

## 10. Mock Data Reuse

**Decision**: Copy mock data files from `apps/driver-web/src/mocks/` to `apps/driver-mobile/src/mocks/` with identical shapes. Type definitions also copied/shared. This keeps each app independent during Phase 1.

## 11. Dependency Installation

**Key packages to install:**

```json
{
  "dependencies": {
    "expo": "~52.0.0",
    "react": "18.3.1",
    "react-native": "0.76.x",
    "@react-navigation/native": "^6.x",
    "@react-navigation/bottom-tabs": "^6.x",
    "@react-navigation/native-stack": "^6.x",
    "react-native-screens": "~4.x",
    "react-native-safe-area-context": "~5.x",
    "react-native-gesture-handler": "~2.x",
    "i18next": "^23.x",
    "react-i18next": "^14.x",
    "expo-localization": "~16.x",
    "expo-font": "~13.x",
    "@expo-google-fonts/plus-jakarta-sans": "^0.x"
  },
  "devDependencies": {
    "@types/react": "~18.3.x",
    "jest": "^29.x",
    "@testing-library/react-native": "^12.x",
    "typescript": "~5.3.x"
  }
}
```
