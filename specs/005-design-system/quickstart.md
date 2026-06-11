# Quickstart: Design System — UI Primitives & Tokens

**Phase**: Phase 1 | **Date**: 2026-06-11 | **Feature**: [spec.md](spec.md)

## Prerequisites

- Node.js 20+
- Expo SDK 54 project (created in Sprint 1.5 or standalone for testing)
- Yarn 4 or npm 10

## Setup

### 1. Create the design-system package

```bash
# From source/front/
mkdir -p packages/design-system/src/{tokens,components/{Button,Skeleton,EmptyState,ErrorState,BottomSheet}}
```

### 2. Initialize package.json

```bash
cd packages/design-system
npm init -y
```

### 3. Install dependencies

```bash
npm install react-native-reanimated expo-haptics
npm install --save-dev typescript @types/react jest @testing-library/react-native
```

### 4. Add scripts to package.json

```json
{
  "name": "@borne/design-system",
  "main": "src/index.ts",
  "scripts": {
    "test": "jest",
    "storybook": "storybook start"
  }
}
```

## Usage

### Import tokens

```tsx
import { colors, spacing, useTheme } from '@borne/design-system/tokens';

function MyScreen() {
  const { palette } = useTheme();
  return (
    <View style={{ backgroundColor: palette.background, padding: spacing.md }}>
      <Text style={{ color: palette.text }}>Hello</Text>
    </View>
  );
}
```

### Import components

```tsx
import { Button, Skeleton, EmptyState } from '@borne/design-system';

function MyScreen({ loading, error, stations }) {
  if (loading) return <Skeleton variant="list" />;
  if (error) return <ErrorState message={error} onRetry={refetch} />;
  if (stations.length === 0) return <EmptyState title="No stations" />;
  return <StationList stations={stations} />;
}
```

## Testing

```bash
# Run all component tests
cd packages/design-system
npm test

# Run Storybook (visual development)
npm run storybook
```

## Verification Checklist

1. [ ] Import `colors.light.primary` — renders correct hex
2. [ ] Switch device to dark mode — colors switch automatically
3. [ ] Render `<Button variant="primary" label="Test" onPress={() => {}} />` — tap triggers haptic
4. [ ] Render `<Skeleton variant="map" />` — shimmer animation visible
5. [ ] Render `<EmptyState title="Test" />` — title renders centered
6. [ ] Render `<ErrorState message="Test" onRetry={() => {}} />` — retry button fires callback
7. [ ] Render `<BottomSheet isOpen={true} onClose={() => {}}><Text>Content</Text></BottomSheet>` — sheet animates up

## Project Structure

```
source/front/packages/design-system/
├── package.json
├── tsconfig.json
└── src/
    ├── tokens/
    │   ├── index.ts
    │   ├── colors.ts
    │   ├── spacing.ts
    │   ├── typography.ts
    │   ├── radii.ts
    │   ├── shadows.ts
    │   └── ThemeContext.tsx
    ├── components/
    │   ├── Button/
    │   │   ├── Button.tsx
    │   │   ├── Button.stories.tsx
    │   │   └── index.ts
    │   ├── Skeleton/
    │   │   ├── Skeleton.tsx
    │   │   ├── Skeleton.stories.tsx
    │   │   └── index.ts
    │   ├── EmptyState/
    │   │   ├── EmptyState.tsx
    │   │   ├── EmptyState.stories.tsx
    │   │   └── index.ts
    │   ├── ErrorState/
    │   │   ├── ErrorState.tsx
    │   │   ├── ErrorState.stories.tsx
    │   │   └── index.ts
    │   └── BottomSheet/
    │       ├── BottomSheet.tsx
    │       ├── BottomSheet.stories.tsx
    │       └── index.ts
    └── index.ts
```
