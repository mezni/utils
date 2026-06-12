# Research: Design System & Components (Phase 3)

## Unknowns Resolved

### 1. Cross-Platform Component Architecture

**Decision**: `.native.tsx` / `.web.tsx` file convention for platform splits, with `react-native-web` shim for shared implementations.

**Rationale**: Metro natively resolves platform file extensions at bundle time — dead code eliminated before reaching the client. `react-native-web` (v0.21.x) maps `View` → `<div>`, `Text` → `<span>`, `Pressable` → clickable element, so simple components need no platform splits at all. Only reach for `.native.tsx`/`.web.tsx` when fundamentally different behavior is needed (e.g., `ScrollView` momentum vs `div` overflow).

**Alternatives considered**:
- `Platform.OS` branching in every component — rejected: runtime checks bloat bundle, scatter platform logic
- Tamagui v2 — rejected: heavy dependency with its own styling DSL, conflicts with existing Tailwind/shadcn direction
- React Native Reusables — noted as compatible pattern but not adopted since we're building owned packages, not consuming a registry

### 2. pnpm Workspace + Expo SDK 54

**Decision**: Default pnpm isolated mode (`nodeLinker: isolated`), no `.npmrc` overrides needed.

**Rationale**: Expo SDK 54 `@expo/metro-config` auto-detects pnpm monorepos and enables symlink following (`unstable_enableSymlinks`). Expo's own monorepo migrated to pnpm in March 2026 (PR #44057). Root `pnpm-workspace.yaml` lists `source/front/packages/*`, `source/front/mobile-driver`, `source/front/web-driver`.

**Gotchas**:
- Pin React/RN versions across workspace to prevent duplicate instance errors
- If issues arise: `experiments.autolinkingModuleResolution: true` in `app.json`
- For `react-native-reanimated`: may need `patchesDir` config for Android build

**Alternatives considered**:
- `nodeLinker: hoisted` — rejected: no longer needed on SDK 54+, defeats pnpm strictness
- npm/yarn workspaces — rejected: Constitution mandates pnpm

### 3. Token Generation Strategy

**Decision**: Write a lightweight Node script to parse `design-system/bornemap/MASTER.md` and generate `@bornemap/tokens/src/*.ts` files. MASTER.md is the canonical source of truth.

**Rationale**: MASTER.md tables are consistently formatted (`| Role | Hex | CSS Variable |`). A ~50-line script can parse them into typed TypeScript constants. This keeps MASTER.md as the source that both designers and devs edit.

**Alternatives considered**:
- Hand-write TypeScript — rejected: duplicates MASTER.md, drifts over time
- UI/UX Pro Max CSV data — rejected: upstream reference library (49 generic schemes), not project-owned
- Style Dictionary — rejected: overengineered for single-platform TypeScript; reconsider if CSS/Android/iOS output needed later

### 4. Component Testing Strategy

**Decision**: Use both `@testing-library/react-native` (RNTL) and `@testing-library/react` (RTL). Core logic tested with RNTL (works in Node/JSDOM with `react-native` preset), platform-specific behavior in separate `*.native.test.tsx` / `*.web.test.tsx` files.

**Rationale**: RNTL queries mirror native accessibility semantics. RTL relies on DOM-specific selectors. Jest can be configured with `react-native` default preset and `/** @jest-environment jsdom */` overrides for web-only files.

**Additional**:
- Chromatic for visual regression testing (natively supports both `@storybook/react` and `@storybook/react-native`)
- Storybook interaction tests for web stories
- RNTL Jest tests for native interaction coverage

**Alternatives considered**:
- Single test suite with `Platform.OS` mocking — rejected: hard to maintain, misses real divergence
- Loki/Percy — rejected: Loki is web-only, Percy RN support less mature than Chromatic

### 5. Documentation Site

**Decision**: Storybook 8 with `@storybook/react-native` addon for cross-platform preview.

**Rationale**: Chromatic CI integration is the most mature for cross-platform visual testing. Storybook is the de facto standard for React component libraries.

**Alternatives considered**:
- Ladle — rejected: web-only, no React Native support
- Custom doc site — rejected: reinventing the wheel, no visual regression built-in

### 6. Package Bundling

**Decision**: Use `tsup` for both packages (ESM output, tree-shaking, TypeScript declaration files).

**Rationale**: tsup bundles with esbuild internally — fast, supports `platform: 'neutral'` for universal React components, generates `.d.ts` files, handles `exports` field in package.json.

**Alternatives considered**:
- `tsc` alone — rejected: doesn't bundle, no tree-shaking of platform variants
- `vite` library mode — viable but tsup is purpose-built for library bundling
- `microbundle` — rejected: less maintained, less configurable

### Dependencies Summary

| Package | Version | Purpose |
|---------|---------|---------|
| TypeScript | 5.5+ | Language |
| pnpm | 9+ | Package manager |
| React | 19 | Web + shared components |
| React Native | 0.76+ | Native mobile |
| expo | SDK 54 | Mobile framework |
| react-native-web | 0.21+ | Web shim for RN components |
| tsup | 8+ | Package bundler |
| Storybook | 8 | Component documentation |
| @testing-library/react | 16+ | Web component tests |
| @testing-library/react-native | 12+ | Native component tests |
| Chromatic | latest | Visual regression |
| jest | 29+ | Test runner |
| @expo/metro-config | SDK 54 | Metro bundler config |
