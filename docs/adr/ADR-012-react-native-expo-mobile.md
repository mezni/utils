# ADR-012: React Native + Expo SDK 54 for Mobile

**Status**: Accepted

**Date**: 2026-01-01

## Context

Driver Mobile App requires iOS and Android support with a map interface and station discovery. Options: React Native with Expo vs. native development vs. Flutter.

## Decision

**Use React Native + Expo SDK 54 exclusively.**

The app is initialized at `source/apps/driver-mobile/` with Expo managed workflow.

**Exact versions locked**:
```
React Native:    0.76.5
React:           18.3.1
Expo Router:     ~4.0.0
expo-location:   ~18.0.0
react-native-maps: 1.18.0
```

**No Expo SDK upgrade without a new ADR.**

## Rationale

- **Code sharing**: Leverage React knowledge from web apps. Shared tokens from `packages/ui`.
- **Managed infrastructure**: Expo handles iOS and Android builds, push, OTA. No native toolchain required.
- **Fast iteration**: `npx expo start` spins up iOS simulator or Android emulator in seconds.
- **Maps**: `react-native-maps` with default provider (Google Maps on Android, Apple Maps on iOS).
- **Location**: `expo-location` for requesting device location with clean permissions UX.

## Consequences

- All design tokens consumed from `source/packages/ui/src/tokens/native.ts` (plain JavaScript).
- **Critical**: When adding a token to `colors.ts`, must sync to `native.ts` in same commit.
- No third-party native modules without approval (increases build complexity).
- Expo SDK upgrades require new ADR (risk of breaking changes in managed workflow).
- EAS Build used for CI builds; secrets managed via EAS secrets.

## Constraints

- **Location permission**: App must handle denied permission gracefully (use Tunisia center, no error modal).
- **Offline**: App works without network by falling back to cached data (future enhancement).

## References

- Constitution section 4: Frontend Applications, Expo SDK Version
- Constitution section 5.4: Token Delivery
- Implementation Plan, Sprint 1.4: Driver Mobile App
