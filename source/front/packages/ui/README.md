# @bornemap/ui

BorneMap UI component library.

## Installation

```bash
pnpm add @bornemap/ui @bornemap/tokens react react-native
```

## Usage

```typescript
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
import { colors as tokenColors } from '@bornemap/tokens';

// Theme provider
function App() {
  return (
    <ThemeProvider mode="system">
      <MainScreen />
    </ThemeProvider>
  );
}

// Button
<Button variant="primary" size="md" onPress={() => {}} loading>
  Submit
</Button>

// Card
<Card variant="elevated" header={<Text>Header</Text>}>
  <Text>Content</Text>
</Card>

// Skeleton
<Skeleton shape="rectangular" width={100} height={200} />

// Empty state
<EmptyState
  title="No data"
  description="Try again later"
  action={{ label: 'Retry', onPress: handleRetry }}
/>

// Loading overlay
<LoadingOverlay visible message="Loading..." cancelable onCancel={handleCancel} />

// Error boundary
<ErrorBoundary fallback={<Text>Error occurred</Text>}>
  <Component />
</ErrorBoundary>

// Badge
<Badge variant="success">Success</Badge>
```

## Components

| Component | Purpose |
|-----------|---------|
| `ThemeProvider` | Theme context provider with light/dark/system modes |
| `Button` | Button with variants, sizes, loading, disabled states |
| `Card` | Card with header/footer, elevation variants |
| `Skeleton` | Loading skeleton with shapes (rect, circ, text) |
| `EmptyState` | Empty state with icon, title, description, action |
| `ErrorBoundary` | Error boundary with fallback and retry |
| `LoadingOverlay` | Loading overlay with message and cancel option |
| `Badge` | Badge with variants and sizes |

## Theme Integration

Use the theme hook:

```typescript
import { useTheme } from '@bornemap/ui';

function Component() {
  const { mode, isDark, resolvedMode } = useTheme();
  // mode: 'light' | 'dark' | 'system'
  // isDark: boolean
  // resolvedMode: 'light' | 'dark'
}
```

## TypeScript

All components are fully typed with JSDoc comments.

```typescript
import type { ButtonProps, CardProps } from '@bornemap/ui';
```

## Building

```bash
pnpm build
```

## License

MIT
