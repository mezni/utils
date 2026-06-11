# Implementation Plan: Design System — UI Primitives & Tokens

**Branch**: `005-design-system` | **Date**: 2026-06-11 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/005-design-system/spec.md`

## Summary

Build a foundational design system for the Borne mobile app: a tokens module for theming (light/dark), and reusable UI primitives (Button, Skeleton, EmptyState, ErrorState, BottomSheet) built with React Native, TypeScript, and Reanimated v3. This is the critical path blocker for all frontend screens (Sprint 1.5+).

## Technical Context

**Language/Version**: TypeScript 5.x

**Primary Dependencies**: react-native (Expo SDK 54), react-native-reanimated v3, expo-haptics, typescript, react-native-safe-area-context

**Storage**: N/A — stateless component library (no data persistence)

**Testing**: Jest + React Native Testing Library (component render tests), Storybook (visual regression / isolated dev)

**Target Platform**: iOS 15+ / Android 12+ (via Expo)

**Project Type**: mobile-app (React Native component library)

**Performance Goals**: 60fps skeleton shimmer, <300ms bottom sheet settle, <50ms button press response

**Constraints**: Reanimated v3 only (no Animated API), dark mode mandatory, zero hardcoded style values, skeleton-first loading

**Scale/Scope**: 6 primitives (tokens, button, skeleton, empty state, error state, bottom sheet), ~15 TypeScript files

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Constitution is a template-only file (`.specify/memory/constitution.md` is unpopulated). No constitution gates to evaluate.

## Project Structure

### Documentation (this feature)

```text
specs/005-design-system/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
└── tasks.md             # Phase 2 output (/speckit.tasks)
```

### Source Code (repository root)

```text
source/front/
└── packages/
    └── design-system/
        ├── package.json
        ├── tsconfig.json
        ├── src/
        │   ├── tokens/
        │   │   ├── index.ts
        │   │   ├── colors.ts
        │   │   ├── spacing.ts
        │   │   ├── typography.ts
        │   │   ├── radii.ts
        │   │   └── shadows.ts
        │   ├── components/
        │   │   ├── Button/
        │   │   │   ├── Button.tsx
        │   │   │   ├── Button.stories.tsx
        │   │   │   └── index.ts
        │   │   ├── Skeleton/
        │   │   │   ├── Skeleton.tsx
        │   │   │   ├── Skeleton.stories.tsx
        │   │   │   └── index.ts
        │   │   ├── EmptyState/
        │   │   │   ├── EmptyState.tsx
        │   │   │   ├── EmptyState.stories.tsx
        │   │   │   └── index.ts
        │   │   ├── ErrorState/
        │   │   │   ├── ErrorState.tsx
        │   │   │   ├── ErrorState.stories.tsx
        │   │   │   └── index.ts
        │   │   └── BottomSheet/
        │   │       ├── BottomSheet.tsx
        │   │       ├── BottomSheet.stories.tsx
        │   │       └── index.ts
        │   └── index.ts
        └── tests/
            └── components/
                ├── Button.test.tsx
                ├── Skeleton.test.tsx
                ├── EmptyState.test.tsx
                ├── ErrorState.test.tsx
                └── BottomSheet.test.tsx
```

**Structure Decision**: Monorepo package under `source/front/packages/design-system/`, consistent with React Native/Expo workspace conventions. Each component in its own directory with co-located story file and index re-export.

## Complexity Tracking

> No Constitution violations — complexity justification not required.
