# Design System Notes

## Current Tokens
- all visual values come from `source/packages/ui/src/tokens/`
- no hardcoded colors, spacing, typography, radius, or shadows in app code

## Brand Colors
- `brand.primary`: `#007943`
- `brand.primaryDark`: `#005c32`
- `brand.sageLight`: `#EAF0E6`
- `brand.glow`: `#00E676`

## Surface Colors
- `surface.background`: `#F8FAF6`
- `surface.card`: `#FFFFFF`
- `surface.sidebar`: `#FFFFFF`
- `surface.mapTerrain`: `#EAF0E6`

## Status Colors
- `status.available`: `#10B981`
- `status.inUse`: `#F59E0B`
- `status.maintenance`: `#EF4444`

## Typography
- Driver apps: Plus Jakarta Sans with Inter fallback
- Dashboard: Inter with system-ui fallback
- Arabic: Cairo with system-ui fallback

## Layout Patterns
- Driver apps: full-bleed map, floating search and filter pills, bottom sheet station card, bottom tab bar with raised center action button
- Dashboard: fixed left sidebar, top header bar, main content on surface background
