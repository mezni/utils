# Data Model: Design System — UI Primitives & Tokens

**Phase**: Phase 1 | **Date**: 2026-06-11 | **Feature**: [spec.md](spec.md)

## Design Tokens

### ColorPalette

```ts
interface ColorPalette {
  primary: string;
  secondary: string;
  background: string;
  surface: string;
  text: string;
  textSecondary: string;
  error: string;
  success: string;
  border: string;
  skeleton: string;
  skeletonHighlight: string;
  overlay: string;
}

interface Theme {
  light: ColorPalette;
  dark: ColorPalette;
}
```

### SpacingScale

```ts
type SpacingToken = 4 | 8 | 12 | 16 | 20 | 24 | 32 | 48 | 64;

interface Spacing {
  xxs: 4;
  xs: 8;
  sm: 12;
  md: 16;
  lg: 20;
  xl: 24;
  xxl: 32;
  xxxl: 48;
  huge: 64;
}
```

### TypographyScale

```ts
interface Typography {
  fontFamily: {
    regular: string;
    medium: string;
    bold: string;
  };
  fontSize: {
    caption: number;
    body: number;
    bodyLarge: number;
    subtitle: number;
    title: number;
    headline: number;
  };
  fontWeight: {
    regular: '400';
    medium: '500';
    semibold: '600';
    bold: '700';
  };
  lineHeight: {
    tight: number;
    normal: number;
    relaxed: number;
  };
}
```

### RadiiScale

```ts
interface Radii {
  none: 0;
  sm: 4;
  md: 8;
  lg: 12;
  xl: 16;
  full: 9999;
}
```

### ShadowPresets

```ts
interface Shadow {
  shadowColor: string;
  shadowOffset: { width: number; height: number };
  shadowOpacity: number;
  shadowRadius: number;
  elevation: number;
}

interface Shadows {
  sm: Shadow;
  md: Shadow;
  lg: Shadow;
  xl: Shadow;
}
```

## Component Props

### Button

| Prop | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `variant` | `'primary' \| 'secondary' \| 'ghost'` | No | `'primary'` | Visual style variant |
| `label` | `string` | Yes | — | Button text |
| `onPress` | `() => void` | Yes | — | Tap handler |
| `disabled` | `boolean` | No | `false` | Disables interaction, dims appearance |
| `loading` | `boolean` | No | `false` | Shows spinner, disables interaction |

### Skeleton

| Prop | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `variant` | `'map' \| 'list'` | Yes | — | Layout variant |
| `rows` | `number` | No | `3` | Number of rows (list variant only) |
| `width` | `number \| string` | No | `'100%'` | Container width |
| `height` | `number \| string` | No | `'100%'` | Container height |

### EmptyState

| Prop | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `title` | `string` | Yes | — | Primary message |
| `description` | `string` | No | — | Secondary explanation |
| `illustration` | `React.ReactNode` | No | — | Custom SVG illustration component |
| `ctaLabel` | `string` | No | — | CTA button label |
| `onCtaPress` | `() => void` | No | — | CTA button handler |

### ErrorState

| Prop | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `message` | `string` | Yes | — | Error description |
| `onRetry` | `() => void` | Yes | — | Retry button handler |

### BottomSheet

| Prop | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `isOpen` | `boolean` | Yes | — | Controls sheet visibility |
| `onClose` | `() => void` | Yes | — | Called when sheet is dismissed |
| `snapPoints` | `[string \| number, string \| number]` | No | `['60%', '85%']` | First and second snap positions |
| `children` | `React.ReactNode` | Yes | — | Sheet content |
| `disableScrollWhenCollapsed` | `boolean` | No | `true` | Locks scroll when sheet is at minimum snap |
