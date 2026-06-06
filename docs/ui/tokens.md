# Design Tokens

BorneMap design tokens are the single source of truth for all visual values across the platform.

## Import Paths

```typescript
// All tokens
import * as tokens from '@borne-map/ui/tokens'

// By category
import { brandPrimary } from '@borne-map/ui/tokens/colors'
import { fontSizeLg } from '@borne-map/ui/tokens/typography'
import { spacing4 } from '@borne-map/ui/tokens/spacing'
import { radiusMd } from '@borne-map/ui/tokens/radius'
import { shadowCard } from '@borne-map/ui/tokens/shadows'
```

## Token Categories

- **Colors** — Brand, semantic, and neutral color values
- **Typography** — Font families, sizes, weights, and line heights
- **Spacing** — 4px base unit scale (0–64px)
- **Radius** — Border radius values (0–9999px)
- **Shadows** — Elevation shadows for card, panel, float, pin
