# ADR-012: React Native + Expo SDK 54

**Status:** Accepted
**Date:** 2026-06-09

## Context

The Driver Mobile App needs to run on both iOS and Android. The framework must support map integration, location services, and shared code with the web apps (design tokens, API clients).

## Decision

Use React Native with Expo SDK 54. Version is pinned — no upgrade without an approved ADR. Expo Router for navigation, react-native-maps 1.18.0 for maps, expo-location ~18.0.0 for device location. Expo managed workflow with native modules.

## Consequences

- Single codebase for iOS and Android
- Expo manages native build complexity
- Shared TypeScript types and token values with web apps
- Expo SDK 54 provides a stable, tested base
- Version lock requires ADR for any SDK upgrade
