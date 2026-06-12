# Package API Contracts: Design System

## @bornemap/tokens

### Installation

```json
{
  "dependencies": {
    "@bornemap/tokens": "workspace:*"
  }
}
```

### Public API

```typescript
// Colors (light + dark mode pairs)
import { colors } from '@bornemap/tokens';
colors.light.primary;    // "#4F46E5"
colors.dark.primary;     // "#818CF8"

// Spacing (4px base unit)
import { spacing } from '@bornemap/tokens';
spacing[4];  // 16

// Typography
import { typography } from '@bornemap/tokens';
typography.font.size.base;  // 16

// Shadows
import { shadows } from '@bornemap/tokens';
shadows.md;  // "0 4px 6px rgba(0,0,0,0.07)"

// Border radius
import { radii } from '@bornemap/tokens';
radii.md;  // 8

// Breakpoints
import { breakpoints } from '@bornemap/tokens';
breakpoints.desktop;  // 1024

// Type helpers
import type { ColorScheme, SpacingKey } from '@bornemap/tokens';
```

### Build Output

```
dist/
├── index.js          # ESM
├── index.d.ts        # Types
├── index.d.ts.map    # Source map
└── index.js.map      # Source map
```

### package.json (exports field)

```json
{
  "name": "@bornemap/tokens",
  "type": "module",
  "exports": {
    ".": {
      "types": "./dist/index.d.ts",
      "import": "./dist/index.js"
    }
  },
  "main": "./dist/index.js",
  "types": "./dist/index.d.ts"
}
```

---

## @bornemap/ui

### Installation

```json
{
  "dependencies": {
    "@bornemap/ui": "workspace:*",
    "@bornemap/tokens": "workspace:*"
  },
  "peerDependencies": {
    "react": "^19.0.0",
    "react-native": "^0.76.0"
  }
}
```

### Public API

```typescript
// Components
import {
  Button,
  Card,
  Skeleton,
  EmptyState,
  ErrorBoundary,
  ThemeProvider,
  LoadingOverlay,
  Badge,
} from '@bornemap/ui';

// Theme context
import { useTheme } from '@bornemap/ui';
const { mode, setMode, isDark } = useTheme();

// Type exports
import type {
  ButtonProps,
  CardProps,
  SkeletonProps,
  EmptyStateProps,
  ThemeMode,
} from '@bornemap/ui';
```

### Usage Examples

```tsx
// App wrapper
import { ThemeProvider } from '@bornemap/ui';

function App() {
  return (
    <ThemeProvider mode="system">
      <MainScreen />
    </ThemeProvider>
  );
}
```

```tsx
// Button with loading state
import { Button } from '@bornemap/ui';

<Button
  variant="primary"
  size="md"
  loading={isSubmitting}
  onPress={handleSubmit}
>
  Start Charging
</Button>
```

```tsx
// Card with skeleton
import { Card, Skeleton } from '@bornemap/ui';

{loading ? (
  <Card variant="default">
    <Skeleton shape="rectangular" height={200} />
    <Skeleton shape="text" lines={3} />
  </Card>
) : (
  <Card variant="elevated" header={<Text>Station Name</Text>}>
    <Text>Station details...</Text>
  </Card>
)}
```

```tsx
// Error recovery pattern
import { ErrorBoundary, EmptyState } from '@bornemap/ui';

<ErrorBoundary fallback={
  <EmptyState
    icon={<AlertIcon />}
    title="Something went wrong"
    description="We couldn't load station data"
    action={{ label: "Try Again", onPress: retry }}
  />
}>
  <StationDetail id={id} />
</ErrorBoundary>
```

### Build Output

```
dist/
├── Button/
│   ├── index.js
│   ├── index.d.ts
│   └── index.d.ts.map
├── Card/
│   └── ...
├── index.js              # Re-exports all components
├── index.d.ts
├── index.js.map
└── index.d.ts.map
```
