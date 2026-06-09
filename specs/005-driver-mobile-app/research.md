# Phase 0 Research: Driver Mobile App

## R01 — Map Library Choice

**Decision**: Use `react-native-maps` (Apple Maps on iOS, Google Maps on Android).

**Rationale**: Most mature React Native map library. Platform-native rendering (not WebView). Supports custom colored markers, callouts, and region control. Compatible with Expo SDK 54.

**Alternatives considered**: `expo-maps` (newer, less mature), WebView-based Leaflet (loses native feel).

## R02 — Location Permission Flow

**Decision**: Use `expo-location` `requestForegroundPermissionsAsync` on mount. If granted → `getCurrentPositionAsync` to center map. If denied → Tunisia fallback (33.8869, 9.5375).

**Rationale**: expo-location handles iOS/Android permission dialogs consistently. Simple async API — no wrapper needed.

**Alternatives considered**: `react-native-permissions` (more flexible but unnecessary), `navigator.geolocation` (deprecated).

## R03 — Navigation Pattern

**Decision**: Use `@react-navigation/native-stack` with typed route params (`StationDetailParams: { stationId: string }`).

**Rationale**: React Navigation is the de facto standard. Native stack provides platform-native transitions. TypeScript generics provide compile-time safety.

**Alternatives considered**: Expo Router (file-based — newer, less battle-tested), plain Stack navigator (native-stack is more performant).

## R04 — Data Fetching Strategy

**Decision**: Use `@tanstack/react-query` for caching, loading/error state management, and refetch-on-focus.

**Rationale**: Automatic caching, deduplication, and refetch-on-foreground are valuable on mobile where network conditions vary. Less boilerplate than plain fetch + useState.

**Alternatives considered**: Plain fetch + useState (used in Driver Web, but mobile benefits from caching), Redux Toolkit Query (overkill for 2 screens).

## R05 — API Connection from Simulator

**Decision**: Use host machine's LAN IP (e.g., `http://192.168.x.x:3001/api`). Detect via `Platform.OS` at build time.

**Rationale**: iOS Simulator can reach localhost but Android Emulator cannot. LAN IP works for both. Matches spec assumption.

**Alternatives considered**: `localhost` with proxy (Android-specific workaround), `10.0.2.2` for Android only, ngrok (external dependency).
