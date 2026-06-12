# Data Model: Design System & Components

## Design Token Categories

### Color Tokens

| Token Name | Light Mode | Dark Mode | Purpose |
|------------|-----------|-----------|---------|
| `color.primary` | #4F46E5 | #818CF8 | Primary brand color, CTAs, active states |
| `color.onPrimary` | #FFFFFF | #0F172A | Text/icon on primary backgrounds |
| `color.secondary` | #6366F1 | #A5B4FC | Secondary actions, less emphasis |
| `color.onSecondary` | #FFFFFF | #0F172A | Text/icon on secondary backgrounds |
| `color.accent` | #EA580C | #F97316 | Call-to-action highlight, special offers |
| `color.onAccent` | #FFFFFF | #0F172A | Text/icon on accent backgrounds |
| `color.background` | #EEF2FF | #0F172A | Page/screen background |
| `color.foreground` | #312E81 | #F8FAFC | Primary text color |
| `color.card` | #FFFFFF | #1E293B | Card, modal, elevated surface background |
| `color.cardForeground` | #312E81 | #F8FAFC | Text on card backgrounds |
| `color.muted` | #EBEEF8 | #334155 | Subtle backgrounds, disabled states |
| `color.mutedForeground` | #64748B | #94A3B8 | Muted/secondary text |
| `color.border` | #C7D2FE | #334155 | Borders, dividers, separators |
| `color.destructive` | #DC2626 | #EF4444 | Errors, destructive actions |
| `color.onDestructive` | #FFFFFF | #FFFFFF | Text/icon on destructive backgrounds |
| `color.success` | #16A34A | #22C55E | Success states, confirmation |
| `color.warning` | #F59E0B | #FBBF24 | Warning states, caution |
| `color.info` | #0891B2 | #22D3EE | Information, help |
| `color.ring` | #4F46E5 | #818CF8 | Focus ring, active selection |

**Validation**: All foreground/background pairs must pass WCAG AA (4.5:1 contrast).

### Spacing Tokens

| Token Name | Value (px) | Usage |
|------------|-----------|-------|
| `spacing.0` | 0 | None |
| `spacing.1` | 4 | Micro spacing, icons padding |
| `spacing.2` | 8 | Tight padding, inner gaps |
| `spacing.3` | 12 | Form element padding |
| `spacing.4` | 16 | Default padding, card inner |
| `spacing.5` | 20 | Section padding |
| `spacing.6` | 24 | Card padding, modal margin |
| `spacing.8` | 32 | Section spacing |
| `spacing.10` | 40 | Large section spacing |
| `spacing.12` | 48 | Screen edge margins |
| `spacing.16` | 64 | Hero spacing |

### Typography Tokens

| Token Name | Value | Usage |
|------------|-------|-------|
| `font.family.sans` | 'Inter', system-ui, sans-serif | Body and UI text |
| `font.family.mono` | 'JetBrains Mono', monospace | Code, technical data |
| `font.size.xs` | 12 | Captions, labels |
| `font.size.sm` | 14 | Small body, helper text |
| `font.size.base` | 16 | Body text |
| `font.size.lg` | 18 | Large body |
| `font.size.xl` | 20 | Subheadings |
| `font.size.2xl` | 24 | Section headings |
| `font.size.3xl` | 30 | Page headings |
| `font.size.4xl` | 36 | Hero headings |
| `font.weight.normal` | 400 | Body text |
| `font.weight.medium` | 500 | Emphasized body |
| `font.weight.semibold` | 600 | Subheadings |
| `font.weight.bold` | 700 | Headings |
| `font.weight.extrabold` | 800 | Display headings |
| `lineHeight.tight` | 1.2 | Headings |
| `lineHeight.normal` | 1.5 | Body text |
| `lineHeight.relaxed` | 1.75 | Long-form reading |

### Shadow / Elevation Tokens

| Token Name | Shadow Value | Usage |
|------------|-------------|-------|
| `shadow.sm` | 0 1px 2px rgba(0,0,0,0.05) | Subtle separation |
| `shadow.md` | 0 4px 6px rgba(0,0,0,0.07) | Card elevation |
| `shadow.lg` | 0 10px 15px rgba(0,0,0,0.1) | Dropdown, modal |
| `shadow.xl` | 0 20px 25px rgba(0,0,0,0.15) | Toast, overlay |

### Border Radius Tokens

| Token Name | Value | Usage |
|------------|-------|-------|
| `radius.none` | 0 | Sharp edges |
| `radius.sm` | 4 | Input fields, buttons |
| `radius.md` | 8 | Cards, modals |
| `radius.lg` | 12 | Large cards, sheets |
| `radius.full` | 9999 | Pills, badges |

### Breakpoint Tokens

| Token Name | Value (px) | Target |
|------------|-----------|--------|
| `breakpoint.mobile` | 0–639 | Phones |
| `breakpoint.tablet` | 640–1023 | Tablets |
| `breakpoint.desktop` | 1024–1439 | Desktop |
| `breakpoint.wide` | 1440+ | Wide screens |

## UI Component Prop Interfaces

### Button

```typescript
interface ButtonProps {
  variant: 'primary' | 'secondary' | 'outline' | 'ghost' | 'destructive';
  size: 'sm' | 'md' | 'lg';
  loading?: boolean;
  disabled?: boolean;
  fullWidth?: boolean;
  onPress: () => void;
  children: React.ReactNode;
  className?: string;    // web only
  style?: ViewStyle;     // native only
}
```

### Card

```typescript
interface CardProps {
  variant: 'default' | 'elevated' | 'interactive';
  header?: React.ReactNode;
  footer?: React.ReactNode;
  onPress?: () => void;  // interactive variant
  children: React.ReactNode;
  className?: string;
  style?: ViewStyle;
}
```

### Skeleton

```typescript
interface SkeletonProps {
  shape: 'rectangular' | 'circular' | 'text';
  width?: number | string;
  height?: number | string;
  lines?: number;        // text variant only
  className?: string;
  style?: ViewStyle;
}
```

### EmptyState

```typescript
interface EmptyStateProps {
  icon?: React.ReactNode;
  title: string;
  description?: string;
  action?: {
    label: string;
    onPress: () => void;
  };
  className?: string;
}
```

### ErrorBoundary

```typescript
interface ErrorBoundaryProps {
  fallback?: React.ReactNode;
  onError?: (error: Error, info: React.ErrorInfo) => void;
  children: React.ReactNode;
}
```

### ThemeProvider

```typescript
type ThemeMode = 'light' | 'dark' | 'system';

interface ThemeProviderProps {
  mode: ThemeMode;
  onModeChange?: (mode: ThemeMode) => void;
  children: React.ReactNode;
}
```

### LoadingOverlay

```typescript
interface LoadingOverlayProps {
  visible: boolean;
  message?: string;
  cancelable?: boolean;
  onCancel?: () => void;
}
```

### Badge

```typescript
interface BadgeProps {
  variant: 'default' | 'success' | 'warning' | 'error' | 'info';
  size: 'sm' | 'md' | 'lg';
  children: React.ReactNode;
  className?: string;
}
```

## Package Dependency Graph

```
@bornemap/tokens (zero runtime dependencies)
  ↑
@bornemap/ui (depends on @bornemap/tokens + React + react-native)
  ↑                    ↑
mobile-driver       web-driver
(Expo SDK 54)       (React 19 + Leaflet)
```

Both apps are built in Phase 4 — Phase 3 delivers only `@bornemap/tokens` and `@bornemap/ui`.
