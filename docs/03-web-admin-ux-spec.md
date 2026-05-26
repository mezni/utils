# BorneMap — Web Admin Portal UX Spec

## 1. Design Tokens

Color configuration blocks are centrally driven from the design system tokens. **Hardcoded text hex codes are strictly barred.**

### Token Source

File: `sources/frontend/packages/ui/tailwind.config.ts`

### Color Tokens

| Token | Value | Usage |
|-------|-------|-------|
| `accent.DEFAULT` | `#22c55e` (green-500) | Primary Brand Indicator |
| `accent.light` | `#4ade80` (green-400) | Hover / active states |
| `accent.dark` | `#16a34a` (green-600) | Pressed / emphasis states |
| `accent.muted` | `#dcfce7` (green-100) | Subtle backgrounds / badges |
| `surface.DEFAULT` | `#ffffff` | Base page background |
| `surface.overlay` | `rgba(255,255,255,0.92)` | Modal / sheet overlays |
| `surface.card` | `#f9fafb` | Card component backgrounds |

### Border Radius Tokens

| Token | Value | Usage |
|-------|-------|-------|
| `rounded-md` | `6px` | Input structures, selection dropdowns (`<SelectSetting/>`) |
| `rounded-lg` | `8px` | Buttons, contextual interaction selectors |
| `rounded-xl` | `0.75rem` (12px) | Dropzones, explicit map component tags, metric chips |
| `rounded-2xl` | `1rem` (16px) | Core layout cards (`<SettingsCard/>`) |

### Spacing Tokens

| Token | Value | Tailwind Equivalent |
|-------|-------|---------------------|
| `component-gap` | `16px` | `gap-4` or `space-y-4` |
| `internal-pad` | `24px` | `p-6` |
| `section-gap` | `32px` | `space-y-8` |

### Shadow Tokens

| Token | Value | Usage |
|-------|-------|-------|
| `shadow-float` | `0 4px 24px rgba(0,0,0,0.10)` | Elevated floating elements |
| `shadow-card` | `0 2px 12px rgba(0,0,0,0.07)` | Card components |

## 2. Map Component Rules

The map container takes the entire viewport canvas layout space. Action sheets, details grids, and filter mechanisms must layer directly over the map canvas.

### BaseMap Configuration

File: `sources/frontend/apps/admin-portal/src/components/map/BaseMap.tsx`

| Property | Value |
|----------|-------|
| Tile URL | `https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png` |
| Attribution | CARTO |
| Default Center | `[33.8869, 9.5375]` (Tunisia) |
| Default Zoom | `7` |

### Station Marker Icon

- **Shape**: Circular div icon, 32x32px
- **Fill**: `#22c55e` (accent.DEFAULT)
- **Border**: 3px solid white
- **Shadow**: `0 2px 8px rgba(0,0,0,0.25)`
- **Inner SVG**: Lightning bolt icon, 14x14, white fill
- **Anchor**: Centered (16, 16)
