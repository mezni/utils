# Data Model: Design System Foundation

## Overview

This feature does not involve a database. The "data model" consists of:
- Design tokens (visual values)
- Component interfaces (props and types)
- Component variants and states

All data is defined in TypeScript files and consumed by applications at runtime.

---

## Design Tokens

### Token Categories

Design tokens are organized into 5 categories, each in its own file.

#### 1. Colors (colors.ts)

**Purpose**: All color values used throughout the platform.

**Entities**:

| Token Name | Type | Value | Description |
|------------|------|-------|-------------|
| `brandPrimary` | string | `"#007943"` | Primary brand color (BorneMap green) |
| `brandSecondary` | string | `"#1e293b"` | Secondary brand color (sage light) |
| `brandLight` | string | `"#EAF0E6"` | Light brand color (background) |
| `brandDark` | string | `"#004d29"` | Dark brand color (contrast) |
| `success` | string | `"#10b981"` | Success state color |
| `warning` | string | `"#f59e0b"` | Warning state color |
| `error` | string | `"#ef4444"` | Error state color |
| `neutral100` | string | `"#f8fafc"` | Neutral light (background) |
| `neutral200` | string | `"#e2e8f0"` | Neutral medium (border) |
| `neutral300` | string | `"#cbd5e1"` | Neutral medium-light |
| `neutral400` | string | `"#94a3b8"` | Neutral medium-dark |
| `neutral500` | string | `"#64748b"` | Neutral dark (text) |
| `neutral600` | string | `"#475569"` | Neutral darker (text-secondary) |
| `neutral700` | string | `"#334155"` | Neutral darkest (border) |

**Validation**:
- All values must be valid CSS hex colors
- Brand colors must have ≥ 4.5:1 contrast ratio on neutral background
- Success/warning/error colors must have ≥ 4.5:1 contrast ratio on neutral background

---

#### 2. Typography (typography.ts)

**Purpose**: Font families, sizes, weights, and line heights.

**Entities**:

| Token Name | Type | Value | Description |
|------------|------|-------|-------------|
| `fontFamilySans` | string | `"Plus Jakarta Sans"` | Primary font for web/mobile |
| `fontFamilyMono` | string | `"Inter"` | Secondary font for dashboard (dense tables) |
| `fontSizeBase` | number | `14` | Base font size (px) |
| `fontSizeSm` | number | `12` | Small size (px) |
| `fontSizeMd` | number | `14` | Medium size (px) |
| `fontSizeLg` | number | `16` | Large size (px) |
| `fontSizeXl` | number | `18` | Extra large size (px) |
| `fontSize2xl` | number | `24` | 2X large size (px) |
| `fontSize3xl` | number | `32` | 3X large size (px) |
| `fontSize4xl` | number | `48` | 4X large size (px) |
| `fontWeightRegular` | number | `400` | Regular weight |
| `fontWeightMedium` | number | `500` | Medium weight |
| `fontWeightSemibold` | number | `600` | Semibold weight |
| `fontWeightBold` | number | `700` | Bold weight |
| `lineHeightTight` | number | `1.25` | Tight line height |
| `lineHeightNormal` | number | `1.5` | Normal line height |
| `lineHeightRelaxed` | number | `1.75` | Relaxed line height |

**Validation**:
- Font sizes must be positive integers (px)
- Font weights must be valid CSS font weights (400, 500, 600, 700)
- Line heights must be positive numbers

---

#### 3. Spacing (spacing.ts)

**Purpose**: Spacing scale based on 4px base unit.

**Entities**:

| Token Name | Type | Value | Description |
|------------|------|-------|-------------|
| `spacing0` | number | `0` | No spacing |
| `spacing1` | number | `4` | 1 unit (4px) |
| `spacing2` | number | `8` | 2 units (8px) |
| `spacing3` | number | `12` | 3 units (12px) |
| `spacing4` | number | `16` | 4 units (16px) |
| `spacing5` | number | `20` | 5 units (20px) |
| `spacing6` | number | `24` | 6 units (24px) |
| `spacing7` | number | `32` | 7 units (32px) |
| `spacing8` | number | `40` | 8 units (40px) |
| `spacing10` | number | `48` | 10 units (48px) |
| `spacing12` | number | `64` | 12 units (64px) |

**Validation**:
- All values must be multiples of 4
- Spacing values must be positive (≥ 0)

---

#### 4. Radius (radius.ts)

**Purpose**: Border radius values for components.

**Entities**:

| Token Name | Type | Value | Description |
|------------|------|-------|-------------|
| `radiusNone` | number | `0` | No border radius |
| `radiusSm` | number | `4` | Small radius |
| `radiusMd` | number | `8` | Medium radius |
| `radiusLg` | number | `16` | Large radius |
| `radiusXl` | number | `24` | Extra large radius |
| `radiusFull` | number | `9999` | Full border radius (pill shape) |

**Validation**:
- All values must be non-negative integers

---

#### 5. Shadows (shadows.ts)

**Purpose**: Shadow values for card, panel, float, and pin effects.

**Entities**:

| Token Name | Type | Value | Description |
|------------|------|-------|-------------|
| `shadowNone` | object | `{ elevation: 0 }` | No shadow |
| `shadowCard` | object | `{ elevation: 2 }` | Card shadow (subtle) |
| `shadowPanel` | object | `{ elevation: 4 }` | Panel shadow (medium) |
| `shadowFloat` | object | `{ elevation: 6 }` | Floating element shadow (strong) |
| `shadowPin` | object | `{ elevation: 8 }` | Pin marker shadow (strongest) |

**React Native Format**:
```typescript
// Example: shadowCard
export const shadowCard = {
  elevation: 2,
  shadowColor: '#000',
  shadowOffset: { width: 0, height: 2 },
  shadowOpacity: 0.1,
  shadowRadius: 4,
  androidElevation: 2
}
```

**Validation**:
- elevation values must be non-negative integers
- shadowColor must be valid hex color
- shadowOffset must have non-negative width and height

---

### Token Index (index.ts)

**Purpose**: Central re-export of all tokens for easy importing.

**Entities**:

```typescript
export * from './colors'
export * from './typography'
export * from './spacing'
export * from './radius'
export * from './shadows'
```

**Validation**: All token files must export all required tokens. Index must re-export everything.

---

### React Native Compatibility (native.ts)

**Purpose**: Token values in React Native StyleSheet-compatible format.

**Entities**:

All token values are converted to React Native format:
- Colors: string (same)
- Spacing: number (4px → 4)
- Shadows: object with `elevation`, `shadowColor`, `shadowOffset`, `shadowOpacity`, `shadowRadius`, `androidElevation`

**Example**:

```typescript
// colors.ts
export const brandPrimary = '#007943'

// native.ts
export const brandPrimary = '#007943'

// spacing.ts
export const spacing4 = 16

// native.ts
export const spacing4 = 16
```

**Validation**: All values must be compatible with React Native StyleSheet.

---

## Component Interfaces

### Button Component

**TypeScript Interface**:

```typescript
type ButtonVariant = 'primary' | 'secondary' | 'ghost' | 'danger'

type ButtonSize = 'sm' | 'md' | 'lg'

type ButtonState = 'default' | 'hover' | 'active' | 'disabled' | 'loading'

interface ButtonProps {
  variant?: ButtonVariant
  size?: ButtonSize
  state?: ButtonState
  disabled?: boolean
  loading?: boolean
  children: React.ReactNode
  onClick?: () => void
}
```

**Validation**:
- `state` prop: if `loading` is true, disable all other states and show loading indicator
- `state="disabled"`: overrides `disabled` prop (force disabled)
- `state="hover"`: for visual testing only, not intended for end-user usage

---

### Input Component

**TypeScript Interface**:

```typescript
type InputVariant = 'default' | 'error' | 'search'

type InputSize = 'sm' | 'md' | 'lg'

type InputState = 'default' | 'focused' | 'error' | 'disabled'

interface InputProps {
  variant?: InputVariant
  size?: InputSize
  state?: InputState
  disabled?: boolean
  error?: string
  placeholder?: string
  value?: string
  onChange?: (value: string) => void
  type?: 'text' | 'password' | 'search'
}
```

**Validation**:
- If `error` is provided, `state` defaults to 'error'
- If `state="disabled"`, `disabled` prop takes precedence
- Search variant should use search icon (icon not specified in this design)

---

### StatusBadge Component

**TypeScript Interface**:

```typescript
type StatusBadgeVariant = 'available' | 'in-use' | 'maintenance' | 'offline'

type StatusBadgeState = 'default' | 'animating'

interface StatusBadgeProps {
  variant: StatusBadgeVariant
  state?: StatusBadgeState
  showDot?: boolean
  children?: React.ReactNode
}
```

**Validation**:
- If `showDot` is true, always show colored dot
- If `showDot` is false, text only
- Non-color indicator (dot) required for accessibility
- State 'animating' indicates blinking/rotating dot (for "available" variant)

---

### Badge Component

**TypeScript Interface**:

```typescript
type BadgeVariant = 'default' | 'success' | 'warning' | 'error' | 'info'

interface BadgeProps {
  variant?: BadgeVariant
  children: React.ReactNode
}
```

**Validation**:
- `variant` defaults to 'default' if not provided

---

### Skeleton Component

**TypeScript Interface**:

```typescript
type SkeletonType = 'block' | 'text' | 'circular'

interface SkeletonProps {
  type: SkeletonType
  width?: number | string
  height?: number | string
  animated?: boolean
}
```

**Validation**:
- `type='block'`: rectangular block, width/height required
- `type='text'`: line of text, width required, height based on line height
- `type='circular'`: circle, width/height required, same value
- `animated` defaults to true

---

### EmptyState Component

**TypeScript Interface**:

```typescript
interface EmptyStateProps {
  icon?: React.ReactNode
  title: string
  description?: string
  action?: {
    label: string
    onClick: () => void
  }
}
```

**Validation**:
- `title` is required
- `description` is optional
- `action` is optional (both label and onClick required if present)

---

### ErrorState Component

**TypeScript Interface**:

```typescript
interface ErrorStateProps {
  icon?: React.ReactNode
  title: string
  description?: string
  retry?: () => void
}
```

**Validation**:
- `title` is required
- `description` is optional
- `retry` is optional (function required if present)

---

### Toast Component

**TypeScript Interface**:

```typescript
type ToastVariant = 'success' | 'error' | 'warning' | 'info'

interface ToastProps {
  variant?: ToastVariant
  title: string
  message?: string
  duration?: number // milliseconds
  onClose?: () => void
  showCloseButton?: boolean
}
```

**Validation**:
- `title` is required
- `message` is optional
- `duration` defaults to 5000ms
- `showCloseButton` defaults to true

---

### Modal Component

**TypeScript Interface**:

```typescript
type ModalSize = 'sm' | 'md' | 'lg'

interface ModalProps {
  size?: ModalSize
  title?: string
  isOpen: boolean
  onClose: () => void
  children: React.ReactNode
}
```

**Validation**:
- `size` defaults to 'md' if not provided
- `title` is optional (modal can have no title)
- `isOpen` and `onClose` are required (controlled component)

---

### Table Component

**TypeScript Interface**:

```typescript
interface TableProps {
  columns: Array<{
    key: string
    label: string
    sortable?: boolean
    width?: string | number
  }>
  data: Array<Record<string, any>>
  onRowAction?: (action: string, rowData: any) => void
  rowActions?: Array<{
    label: string
    icon: React.ReactNode
  }>
}
```

**Validation**:
- `columns` array required
- `data` array required
- `onRowAction` is optional
- `rowActions` is optional

---

### StatCard Component

**TypeScript Interface**:

```typescript
interface StatCardProps {
  label: string
  value: string | number
  trend?: {
    value: number // percentage change
    positive?: boolean // default true
  }
  icon?: React.ReactNode
}
```

**Validation**:
- `label` is required
- `value` is required
- `trend` is optional (both value and positive required if present)
- `icon` is optional

---

### DataCard Component

**TypeScript Interface**:

```typescript
interface DataCardProps {
  title?: string
  action?: {
    label: string
    onClick: () => void
  }
  children: React.ReactNode
}
```

**Validation**:
- `title` is optional
- `action` is optional (both label and onClick required if present)
- `children` is required (content slot)

---

## Component Variants & States

### Variant Definition Pattern

Each component defines its variants using TypeScript union types:

```typescript
// Example from Button
type ButtonVariant = 'primary' | 'secondary' | 'ghost' | 'danger'
```

### State Definition Pattern

Each component defines its states using TypeScript union types:

```typescript
// Example from Button
type ButtonState = 'default' | 'hover' | 'active' | 'disabled' | 'loading'
```

**Rules**:
- Variants define intentional visual changes (e.g., Button primary)
- States define dynamic runtime conditions (e.g., Button disabled)
- Each variant/state combination is tested individually (FR-011)

---

## Validation Summary

**Token Validation**:
- All token values must match their declared types
- Colors must be valid CSS hex colors
- Spacing values must be multiples of 4
- Shadows must have valid elevation and shadow color values

**Component Validation**:
- All props must be optional unless marked required
- Props with defaults must document them
- Variants and states must be typed with union types
- Accessibility requirements must be met for all states (e.g., focus indicators, ARIA labels)

**Integration Validation**:
- All tokens must be exported from index.ts
- All components must be exported from components/index.ts
- No hardcoded visual values (all from tokens)
- React Native compatibility maintained in native.ts

---

**Document Version**: 1.0
**Status**: Active
**Last Updated**: 2026-06-05
