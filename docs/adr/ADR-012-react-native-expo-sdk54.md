# ADR-012: React Native + Expo SDK 54 for Mobile App

**Status**: Accepted
**Date**: 2026-06-07

## Context

The driver mobile application needs to run on iOS and Android. Options: pure React Native CLI, Expo managed workflow, Flutter, Kotlin Multiplatform.

## Decision

Use React Native with Expo SDK 54 managed workflow.

## Rationale

- Expo provides a managed workflow that simplifies build and deployment
- SDK 54 pins React Native 0.76.5 and React 18.3.1 — stable versions
- Expo Router provides file-based navigation
- expo-location for GPS access
- react-native-maps for map rendering (uses platform default providers — Apple Maps on iOS, Google Maps on Android)
- Over-the-air updates via EAS Update
- Eliminates native build configuration complexity

## Consequences

- Expo SDK upgrades are breaking changes and require ADR
- Some native modules require expo-dev-client for custom native code
- Application size is larger than pure RN CLI
- Limited to Expo's supported native module set

## Compliance

- Expo SDK version is pinned to 54
- No SDK upgrade without an approved ADR
- Tokens consumed from packages/ui/native
- No paid map providers without an ADR
