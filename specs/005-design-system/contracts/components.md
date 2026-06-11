# Component Contracts: Design System

## Token Exports

```ts
// src/tokens/index.ts
export { colors } from './colors';
export { spacing } from './spacing';
export { typography } from './typography';
export { radii } from './radii';
export { shadows } from './shadows';
export { ThemeProvider, useTheme } from './ThemeContext';
```

```ts
// Usage
import { colors, spacing, typography, useTheme } from '@borne/design-system/tokens';

const { palette } = useTheme(); // returns active palette (light or dark)
const styles = StyleSheet.create({
  container: {
    backgroundColor: palette.background,
    padding: spacing.md,
  },
  title: {
    fontSize: typography.fontSize.title,
    fontFamily: typography.fontFamily.bold,
    color: palette.text,
  },
});
```

## Button Contract

```tsx
// <Button variant="primary" label="Retry" onPress={handleRetry} />
import { Button } from '@borne/design-system';

<Button variant="primary" label="Find Stations" onPress={handleSearch} />
<Button variant="secondary" label="Cancel" onPress={handleCancel} />
<Button variant="ghost" label="Skip" onPress={handleSkip} />
<Button variant="primary" label="Loading..." loading onPress={handleSubmit} />
<Button variant="primary" label="Disabled" disabled onPress={handleDisabled} />
```

**Behavior**:
- Press triggers scale-down animation (0.97x) + haptic impact
- Loading state: hides label, shows ActivityIndicator, ignores taps
- Disabled state: 0.4 opacity, no animation, no haptic, ignores taps

## Skeleton Contract

```tsx
// Map skeleton — full-screen placeholder
import { Skeleton } from '@borne/design-system';
<Skeleton variant="map" />

// List skeleton — 5 rows
<Skeleton variant="list" rows={5} />
```

**Behavior**:
- Shimmer animation: gradient sweep left-to-right, ~1.5s loop
- Map skeleton: full viewport rectangle with rounded corners
- List skeleton: rows with circle avatar + text lines, staggered shimmer

## EmptyState Contract

```tsx
import { EmptyState } from '@borne/design-system';

// No stations nearby
<EmptyState
  title="No stations nearby"
  description="Try expanding your search area or check back later"
  ctaLabel="Refresh"
  onCtaPress={handleRefresh}
/>

// GPS unavailable (no CTA needed)
<EmptyState
  title="GPS unavailable"
  description="Enable location services to find nearby stations"
/>
```

**Behavior**:
- Renders vertically centered: illustration → title → description → optional CTA
- CTA uses the Button (primary variant) internally

## ErrorState Contract

```tsx
import { ErrorState } from '@borne/design-system';

<ErrorState
  message="Unable to load stations. Please check your connection."
  onRetry={handleRetry}
/>
```

**Behavior**:
- Renders: error icon → message → "Retry" button
- onRetry fires immediately on tap; parent is responsible for re-fetch + re-render

## BottomSheet Contract

```tsx
import { BottomSheet } from '@borne/design-system';

<BottomSheet isOpen={isOpen} onClose={handleClose}>
  <StationDetail station={selectedStation} />
</BottomSheet>
```

**Behavior**:
- Animates in with spring from bottom edge
- Two snap positions: 60% and 85% of screen height
- Swipe down on handle area dismisses
- Scrollable content detected automatically; sheet passes gesture conflicts gracefully
- Calls onClose when swipe-down completes past dismiss threshold (30% of snap distance)
