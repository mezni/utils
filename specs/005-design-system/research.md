# Research: Design System — UI Primitives & Tokens

**Phase**: Phase 0 | **Date**: 2026-06-11 | **Feature**: [spec.md](spec.md)

## 1. Design Token Architecture

**Decision**: Single `tokens.ts` module exporting typed token objects organized by category. Colors use a `{ light, dark }` pair format for automatic theme switching. Tokens are plain TypeScript objects (no runtime CSS-in-JS framework) — consumed via `useStyle` hook or direct import.

**Rationale**: Plain TS objects are zero-cost, fully type-safe, and framework-agnostic. Unlike Stitches or styled-components, they add no bundle size and no runtime overhead. For React Native, where there's no CSS DOM, a plain object approach is the standard pattern (used by Shopify Restyle, NativeBase, etc.).

**Token structure**:
```ts
// colors.ts
export const colors = {
  light: {
    primary: '#...',
    background: '#...',
    surface: '#...',
    text: '#...',
    error: '#...',
    success: '#...',
  },
  dark: {
    primary: '#...',
    // ...
  },
};
```

**Alternatives considered**:
- **Stitches (CSS-in-JS)**: Adds runtime overhead and bundle size. Not needed since React Native doesn't use CSS.
- **Restyle (Shopify)**: Good but adds a dependency. Plain tokens are sufficient for MVP-1.
- **ThemeContext (React Context)**: Used for runtime theme switching. Tokens are wrapped in a ThemeProvider that reads `useColorScheme()` and provides the active palette to all consumers.

## 2. Dark Mode Strategy

**Decision**: Use React Native's `Appearance.addChangeListener` + React Context to provide active theme. No manual toggle in MVP-1.

**Rationale**: The spec assumes system-level dark mode detection (Appearance API). A `ThemeProvider` component wraps the app, listens to system theme changes via `useColorScheme()`, and passes the resolved palette to consumers. This is the zero-effort approach that covers the MVP requirement.

**Alternatives considered**:
- **Manual toggle (switch in settings)**: Overkill for MVP-1. Adds a settings screen dependency. Deferred to post-MVP.
- **Theme persistence (AsyncStorage)**: Needed if manual toggle is added. Not needed for system-only detection.

## 3. Animation Library: Reanimated v3

**Decision**: Reanimated v3 worklets on the UI thread for all animations (skeleton shimmer, bottom sheet, button press).

**Rationale**: The MVP UX rules mandate Reanimated v3 only. It runs animations on the UI thread, avoiding JS thread bottlenecks. Critical for 60fps skeleton shimmer and responsive bottom sheet gestures.

**Key APIs**:
- `useSharedValue`, `useAnimatedStyle` — shared values for animation state
- `withSpring`, `withTiming` — animation drivers
- `GestureDetector` + `Gesture.Pan()` — bottom sheet drag gesture
- `useAnimatedReaction` — reactive side effects

**Alternatives considered**: React Native `Animated` API (blocked by MVP rules — runs on JS thread, janky at 60fps).

## 4. Haptic Feedback for Button

**Decision**: `expo-haptics` `impactAsync` with medium impact weight on button press.

**Rationale**: `expo-haptics` is already part of the Expo SDK ecosystem (no extra install). Medium impact is the standard "button tap" feedback — noticeable but not overwhelming.

**Alternatives considered**:
- **react-native-haptic-feedback**: Requires linking. Not needed since expo-haptics handles all common patterns.
- **No haptics**: Violates FR-007.

## 5. Bottom Sheet Architecture

**Decision**: Custom Reanimated v3 bottom sheet with spring animation, gesture-driven swipe-to-dismiss, and configurable snap points. No third-party bottom sheet library.

**Rationale**: The spec requires Reanimated v3 integration and configurable snap points. The most popular libraries (@gorhom/bottom-sheet) abstract away Reanimated details, but they add a dependency and may not align with our exact API needs. A custom implementation with ~100 lines of code gives full control and zero extra deps.

**Implementation approach**:
- `useSharedValue(0)` for sheet translateY
- `useAnimatedStyle` to transform the sheet container
- `GestureDetector` with `Gesture.Pan()` for drag
- `withSpring` for open/close animation
- Scrollable content via nested `ScrollView` with simultaneous gesture handling

**Alternatives considered**:
- **@gorhom/bottom-sheet**: Battle-tested but opaque API. Adds 30KB to bundle.
- **react-native-actions-sheet**: Similar tradeoffs.

## 6. Component Prop Patterns

**Decision**: Each component exports a `Props` TypeScript interface with JSDoc comments. Components are pure (no side effects, no internal state except animation values).

```ts
interface ButtonProps {
  variant?: 'primary' | 'secondary' | 'ghost';
  disabled?: boolean;
  loading?: boolean;
  onPress: () => void;
  label: string;
}
```

**Rationale**: Pure functional components are easier to test, storybook-ize, and compose. All stateful concerns (loading, disabled) are controlled by the parent.

## 7. Testing Strategy

**Decision**: Jest + React Native Testing Library for component render/interaction tests. Storybook for visual dev and manual verification.

**Rationale**: RNTL provides realistic component rendering without a device. Good for testing: render output, prop-driven variants, callback invocation. Storybook fills the gap for visual/animation verification that automated tests can't cover.

**Test coverage per component**:
- Button: renders all variants, fires onPress, disabled blocks press, shows spinner when loading
- Skeleton: renders map layout, renders list layout, shimmer animation runs
- EmptyState: renders with custom title/description, renders with CTA, fires onCtaPress
- ErrorState: renders error message, fires onRetry
- BottomSheet: renders children, animates open/close, gesture dismiss
