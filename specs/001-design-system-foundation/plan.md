# Implementation Plan: Design System Foundation

**Branch**: `001-design-system-foundation` | **Date**: 2026-06-05 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/001-design-system-foundation/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/plan-template.md` for the execution workflow.

## Summary

Create a shared design system foundation for BorneMap platform consisting of design tokens and 12 reusable UI components. The design token package defines all visual values (colors, typography, spacing, shadows, radius) as TypeScript constants consumed across all applications. The component package provides foundational React components that use tokens for all visual styling. This enables rapid, consistent UI development while adhering to the Visual Consistency principle and eliminating visual inconsistencies caused by manual token definition.

## Technical Context

**Language/Version**: TypeScript 5.x with strict mode

**Primary Dependencies**: React 18+, Tailwind CSS, React Native, TypeScript, ESLint, Prettier, Vitest, @testing-library/react

**Storage**: None (pure frontend library/package)

**Testing**: Vitest (unit tests), @testing-library/react (component tests), React Native testing utilities

**Target Platform**: Web (React + Tailwind), React Native (mobile), Dashboard (React + Tailwind)

**Project Type**: Monorepo library package within pnpm workspace

**Performance Goals**: Zero build warnings, component renders in <100ms, instant token resolution

**Constraints**:
- All components must be TypeScript for type safety
- React Native compatibility requires token values to be StyleSheet-compatible
- Tailwind config must extend all token values without errors
- All visual values must come from tokens (hardcoding prohibited)
- WCAG 2.1 AA accessibility compliance for web applications
- Arabic RTL support must work automatically without manual handling
- Components must be unit tested per variant/state combination

**Scale/Scope**: 12 shared components, 5 token categories (colors, typography, spacing, radius, shadows), single source of truth consumed by 3 applications

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### Principle I: Pragmatic Architecture
✅ **PASS** - Design token package and shared components are minimal, well-defined, and justified by the need for visual consistency across 3 applications. No unnecessary complexity.

### Principle II: Single Source of Truth
✅ **PASS** - `packages/ui` is the single source of truth for all visual values. All three applications consume from this package.

### Principle III: Simple Operations
✅ **PASS** - Package structure is simple (token files + components), using standard Node.js tooling (pnpm, TypeScript). No complex operational requirements.

### Principle IV: Domain Separation by Schema
N/A - No database schemas involved in this feature.

### Principle V: Build for Current Scale
✅ **PASS** - Token system scales naturally. Components are foundational - no premature optimization. Simple implementation that can grow incrementally.

### Principle VI: Public Access First
N/A - No authentication or user access gates in this feature.

### Principle VII: RTL & Arabic Built-In
✅ **PASS** - Components must support RTL automatically based on context. RTL failures would be Class A bugs per spec edge cases.

### Principle VIII: Visual Consistency
✅ **PASS** - Core objective. Token system ensures single source of truth. All visual values from `packages/ui`.

### Non-Negotiable Rules
- ✅ **inventory.station** - Not applicable
- ✅ **Public access** - Not applicable
- ✅ **Tokens not stored** - Not applicable (these are design tokens, not auth tokens)
- ✅ **Arabic RTL** - Must work automatically
- ✅ **Only Traefik** - Not applicable
- ✅ **Keycloak owns auth** - Not applicable
- ✅ **No additional services without ADR** - Package is justified by Visual Consistency principle and project scope
- ✅ **Cross-schema access** - Not applicable

**Overall Result**: ✅ **ALL GATES PASSED**

## Project Structure

### Documentation (this feature)

```text
specs/001-design-system-foundation/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)

```text
# Design System Package
packages/ui/
├── src/
│   ├── tokens/
│   │   ├── colors.ts          # Color token definitions
│   │   ├── typography.ts      # Font families, sizes, weights, line heights
│   │   ├── spacing.ts         # Spacing scale (4px base unit)
│   │   ├── radius.ts          # Border radius values
│   │   ├── shadows.ts         # Card, panel, float, pin shadows
│   │   ├── index.ts           # Re-exports all tokens
│   │   └── native.ts          # React Native StyleSheet-compatible exports
│   ├── components/
│   │   ├── Button/
│   │   │   ├── Button.tsx
│   │   │   └── Button.test.tsx
│   │   ├── Input/
│   │   │   ├── Input.tsx
│   │   │   └── Input.test.tsx
│   │   ├── Badge/
│   │   │   ├── Badge.tsx
│   │   │   └── Badge.test.tsx
│   │   ├── StatusBadge/
│   │   │   ├── StatusBadge.tsx
│   │   │   └── StatusBadge.test.tsx
│   │   ├── Skeleton/
│   │   │   ├── Skeleton.tsx
│   │   │   └── Skeleton.test.tsx
│   │   ├── EmptyState/
│   │   │   ├── EmptyState.tsx
│   │   │   └── EmptyState.test.tsx
│   │   ├── ErrorState/
│   │   │   ├── ErrorState.tsx
│   │   │   └── ErrorState.test.tsx
│   │   ├── Toast/
│   │   │   ├── Toast.tsx
│   │   │   └── Toast.test.tsx
│   │   ├── Modal/
│   │   │   ├── Modal.tsx
│   │   │   └── Modal.test.tsx
│   │   ├── Table/
│   │   │   ├── Table.tsx
│   │   │   └── Table.test.tsx
│   │   ├── StatCard/
│   │   │   ├── StatCard.tsx
│   │   │   └── StatCard.test.tsx
│   │   └── DataCard/
│   │       ├── DataCard.tsx
│   │       └── DataCard.test.tsx
│   ├── index.ts           # Re-exports all components
│   └── types.ts           # TypeScript types and interfaces
├── tailwind.config.base.js  # Tailwind config extending token values
├── package.json
├── tsconfig.json
├── tsconfig.node.json
├── vite.config.js
├── .eslintrc.js
├── .prettierrc
└── README.md

# Documentation (updated in this feature)
docs/ui/
├── components.md   # Documentation for all shared components
├── tokens.md       # Documentation for design tokens
└── design-tokens.md # Updated with concrete token values
```

**Structure Decision**: This is a library/package structure within a monorepo. The `packages/ui` package is self-contained with no external dependencies beyond React, TypeScript, and testing tools. It provides pure TypeScript exports for tokens and React components for all BorneMap applications to consume. This aligns with the Visual Consistency principle by ensuring a single source of truth for all visual values.

## Complexity Tracking

N/A - No constitution violations that require justification.
