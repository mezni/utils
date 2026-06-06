# @borne-map/ui

Shared design system for BorneMap platform — design tokens and React components.

## Installation

```bash
pnpm add @borne-map/ui
```

## Usage

### Tokens

```typescript
import { brandPrimary, spacing4, radiusMd } from '@borne-map/ui/tokens'
// Use directly: brandPrimary → '#007943'
```

### Components

```tsx
import { Button, Input, Badge } from '@borne-map/ui'

<Button variant="primary" onClick={() => {}}>
  Click me
</Button>
```

### Tailwind

```javascript
// tailwind.config.js
import config from '@borne-map/ui/tailwind.config.base.js'
export default config
```

## Tokens

5 categories: colors, typography, spacing, radius, shadows. See `docs/ui/tokens.md`.

## Components

12 components. See `docs/ui/components.md`.

## Build

```bash
pnpm build    # TypeScript type-check
pnpm test     # Run vitest
pnpm lint     # ESLint
```
