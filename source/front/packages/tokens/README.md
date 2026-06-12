# @bornemap/tokens

BorneMap design system tokens package.

## Installation

```bash
pnpm add @bornemap/tokens
```

## Usage

```typescript
import { colors, spacing, typography, shadows, radii, breakpoints } from '@bornemap/tokens';

// Colors
const primary = colors.light.primary; // "#4F46E5"

// Spacing
const padding = spacing[4]; // 16

// Typography
const fontSizeBase = typography.font.size.base; // 16
const fontFamily = typography.font.family.sans; // "Inter, system-ui, sans-serif"

// Shadows
const cardShadow = shadows.md; // "0 4px 6px rgba(0,0,0,0.07)"

// Border radius
const radius = radii.md; // 8

// Breakpoints
const desktop = breakpoints.desktop; // 1024
```

## Available Exports

### Token Categories
- `colors`: Light and dark color palettes
- `spacing`: 4px-base spacing scale
- `typography`: Font family, sizes, weights, line heights
- `shadows`: Elevation tokens
- `radii`: Border radius tokens
- `breakpoints`: Responsive breakpoints
- `opacity`: Opacity levels
- `iconSize`: Icon dimension tokens

### Type Exports
- `ColorScheme`: Color role interface
- `SpacingKey`: Spacing token type
- `TypographyTokens`: Typography interfaces
- `ShadowTokens`: Shadow interfaces
- `RadiiTokens`: Radius interfaces
- `BreakpointTokens`: Breakpoint interfaces
- `OpacityTokens`: Opacity interfaces
- `IconSizeTokens`: Icon size interfaces

## API Reference

See `src/index.ts` for complete API documentation.

## Building

```bash
pnpm build
```

## License

MIT
