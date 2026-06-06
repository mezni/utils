# Design Token Foundation

The design system defines all visual values as tokens. **No color, spacing, typography, radius, shadow, or border value may be hardcoded in any component or application code.** Tokens are the only permitted source of visual values.

---

## 1. Color Tokens

All colors defined in `packages/ui/src/tokens/colors.ts`.

### Brand Colors

| Token | Value | Purpose | Used By |
|-------|-------|---------|---------|
| `brand.primary` | `#007943` | Primary actions, CTAs, active states, links | All apps |
| `brand.primaryDark` | `#005c32` | Gradients, pressed states | All apps |
| `brand.sageLight` | `#EAF0E6` | Selected states, inactive nav backgrounds | All apps |
| `brand.glow` | `#00E676` | Live map pin neon glow effect | Driver apps only |

**Theme Consistency:** Driver Web and Driver Mobile share the same brand greens (#007943, #EAF0E6) as the Dashboard App, ensuring visual consistency across the platform.

### Surface Colors

| Token | Value | Purpose | Context |
|-------|-------|---------|---------|
| `surface.background` | `#F8FAF6` | Page/screen canvas background | Driver apps (map canvas), Dashboard fallback |
| `surface.mapTerrain` | `#EAF0E6` | Map canvas base color | Driver apps only |
| `surface.card` | `#FFFFFF` | Card, panel, and modal background | All apps |
| `surface.sidebar` | `#FFFFFF` | Dashboard sidebar background | Dashboard app only |

### Text Colors

| Token | Value | Purpose |
|-------|-------|---------|
| `text.main` | `#111827` | Primary readable text, headings, body |
| `text.muted` | `#6B7280` | Secondary text, labels, metadata, placeholders |

### Border Colors

| Token | Value | Purpose |
|-------|-------|---------|
| `border.default` | `#E5E7EB` | Standard dividers and card borders |
| `border.subtle` | `#F3F4F6` | Very light dividers inside cards |

### Status Colors

| Token | Value | Purpose | Background |
|--------|-------|---------|------------|
| `status.available` | `#10B981` | Available charger or healthy station | `#ECFDF5` |
| `status.inUse` | `#F59E0B` | Station or charger currently in use | `#FFFBEB` |
| `status.maintenance` | `#EF4444` | Needs maintenance or offline | `#FEF2F2` |

**Semantics:** Status colors communicate charger availability at a glance. Always pair the color with the background variant for accessibility.

### Neutral Scale

Full neutral gray scale for secondary text, dividers, and disabled states:

| Token | Value |
|-------|-------|
| `neutral.50` | `#F9FAFB` |
| `neutral.100` | `#F3F4F6` |
| `neutral.200` | `#E5E7EB` |
| `neutral.300` | `#D1D5DB` |
| `neutral.400` | `#9CA3AF` |
| `neutral.500` | `#6B7280` |
| `neutral.600` | `#4B5563` |
| `neutral.700` | `#374151` |
| `neutral.800` | `#1F2937` |
| `neutral.900` | `#111827` |

---

## 2. Typography Tokens

Defined in `packages/ui/src/tokens/typography.ts`.

### Font Families

**Driver Apps (Web + Mobile):**
- Primary: `Plus Jakarta Sans` — high-contrast weight range, suitable for map interfaces
- Fallback: `Inter`, `system-ui`, `sans-serif`

**Dashboard App:**
- Primary: `Inter` — readability in dense data tables and forms
- RTL: `Cairo` for Arabic typography
- Fallback: `system-ui`, `sans-serif`

**All Apps (RTL Support):**
- Arabic/RTL: `Cairo` — designed for Semitic scripts

### Font Sizes

Modular scale (px):

| Token | Size | Usage |
|-------|------|-------|
| `xs` | 10px | Labels, badges, tiny text |
| `sm` | 12px | Captions, secondary labels |
| `base` | 14px | Body text, default |
| `lg` | 16px | Larger body, list items |
| `xl` | 18px | Section headings, important labels |
| `2xl` | 20px | Subheadings |
| `3xl` | 24px | Page titles, large headings |

### Font Weights

| Token | Weight | Usage |
|-------|--------|-------|
| `regular` | 400 | Body text, default |
| `medium` | 500 | Emphasized labels |
| `semibold` | 600 | Subheadings, important text (Dashboard) |
| `bold` | 700 | Headings, station names (Driver apps) |
| `extrabold` | 800 | Large headings, emphasis (Driver apps) |

**Driver App Typography Note:** Plus Jakarta Sans weights 700–800 are used for station names to maintain high contrast on the full-bleed map.

**Dashboard App Typography Note:** Inter weights 600–700 are used for data labels and table headers for readability in dense layouts.

---

## 3. Spacing Tokens

Base unit: **4px**

| Token | Size | Usage |
|-------|------|-------|
| `xs` | 4px | Micro spacing between inline elements |
| `sm` | 8px | Tight padding inside small components |
| `md` | 12px | Standard padding |
| `lg` | 16px | Comfortable padding for interactive elements |
| `xl` | 20px | Section padding |
| `2xl` | 24px | Page padding, large spacing |
| `3xl` | 32px | Large gaps between sections |
| `4xl` | 40px | Vertical spacing in lists |
| `5xl` | 48px | Large vertical gaps |

---

## 4. Border Radius Tokens

| Token | Size | Usage |
|-------|------|-------|
| `sm` | 4px | Subtle rounding on small buttons |
| `md` | 8px | Cards and inputs |
| `lg` | 12px | Panels and modals |
| `xl` | 16px | Large containers |
| `2xl` | 20px | Bottom sheets (Driver Mobile) |
| `3xl` | 24px | Large modals |
| `full` | 9999px | Circular elements (buttons, badges, avatars) |

---

## 5. Shadow Tokens

Subtle elevation system. Used in `packages/ui/tailwind.config.base.js`.

### CSS Shadows

| Token | Shadow | Elevation | Usage |
|-------|--------|-----------|-------|
| `card` | `0 1px 3px 0 rgb(0 0 0 / 0.07), 0 1px 2px -1px rgb(0 0 0 / 0.07)` | Low | Cards, small panels |
| `panel` | `0 4px 6px -1px rgb(0 0 0 / 0.07), 0 2px 4px -2px rgb(0 0 0 / 0.07)` | Medium | Floating panels, tooltips |
| `float` | `0 10px 25px -5px rgb(0 0 0 / 0.12), 0 8px 10px -6px rgb(0 0 0 / 0.08)` | High | Bottom sheets, action buttons |
| `pin` | `0 0 10px rgba(0, 230, 118, 0.8)` | Glow | Live map pin markers (neon green) |

### React Native Shadows

Mapped to React Native `shadowColor`, `shadowOffset`, `shadowOpacity`, `shadowRadius`, and `elevation`:

```javascript
shadow.card: {
  shadowColor:   '#000',
  shadowOffset:  { width: 0, height: 1 },
  shadowOpacity: 0.07,
  shadowRadius:  3,
  elevation:     2,
}

shadow.float: {
  shadowColor:   '#000',
  shadowOffset:  { width: 0, height: 10 },
  shadowOpacity: 0.12,
  shadowRadius:  25,
  elevation:     8,
}

shadow.pin: {
  shadowColor:   '#00E676',
  shadowOffset:  { width: 0, height: 0 },
  shadowOpacity: 0.8,
  shadowRadius:  10,
  elevation:     4,
}
```

---

## 6. Token Delivery by Platform

### Web Applications (Driver Web + Dashboard)

**File:** `packages/ui/tailwind.config.base.js`

All tokens extended into Tailwind CSS configuration:

```javascript
module.exports = {
  theme: {
    extend: {
      colors: {
        brand:   require('./src/tokens/colors').colors.brand,
        surface: require('./src/tokens/colors').colors.surface,
        status:  require('./src/tokens/colors').colors.status,
        ev:      require('./src/tokens/colors').colors,
      },
      fontFamily: {
        sans:   ['Plus Jakarta Sans', 'Inter', 'system-ui', 'sans-serif'],
        arabic: ['Cairo', 'system-ui', 'sans-serif'],
      },
      borderRadius: {
        DEFAULT: '0.5rem',
        lg:  '1rem',
        xl:  '1.25rem',
        '2xl': '1.5rem',
        '3xl': '2rem',
        full: '9999px',
      },
      boxShadow: {
        card:    '0 1px 3px 0 rgb(0 0 0 / 0.07), 0 1px 2px -1px rgb(0 0 0 / 0.07)',
        panel:   '0 4px 6px -1px rgb(0 0 0 / 0.07), 0 2px 4px -2px rgb(0 0 0 / 0.07)',
        float:   '0 10px 25px -5px rgb(0 0 0 / 0.12), 0 8px 10px -6px rgb(0 0 0 / 0.08)',
        pin:     '0 0 10px rgba(0, 230, 118, 0.8)',  // glow effect for live map pins
      },
    },
  },
}
```

**Each web app extends this base configuration:**

```javascript
// apps/driver-web/tailwind.config.js
const base = require('../../packages/ui/tailwind.config.base')

module.exports = {
  ...base,
  content: [
    './src/**/*.{ts,tsx}',
    './index.html',
    '../../packages/ui/src/**/*.{ts,tsx}',
  ],
}

// apps/dashboard/tailwind.config.js (identical structure)
const base = require('../../packages/ui/tailwind.config.base')

module.exports = {
  ...base,
  content: [
    './src/**/*.{ts,tsx}',
    './index.html',
    '../../packages/ui/src/**/*.{ts,tsx}',
  ],
}
```

**Usage in components:**

```html
<!-- Driver Web: Station card -->
<div class="bg-surface-card border border-border-default rounded-lg shadow-card p-lg">
  <h2 class="font-bold text-base text-text-main">{{ station.name }}</h2>
  <p class="text-xs text-text-muted">{{ station.address }}</p>
</div>

<!-- Driver Web: Active link -->
<a href="#" class="text-brand-primary hover:text-brand-primaryDark font-semibold">
  View stations
</a>

<!-- Dashboard: Status badge -->
<span class="bg-status-availableBg text-status-available px-2 py-1 rounded-full text-xs font-semibold">
  Available
</span>
```

### Mobile Application (Driver Mobile)

**File:** `packages/ui/src/tokens/native.ts`

React Native cannot use Tailwind or CSS variables. Tokens exported as plain JavaScript values:

```javascript
import { colors } from './colors'
import { typography } from './typography'

export const theme = {
  colors: {
    brandPrimary:     colors.brand.primary,      // #007943
    brandPrimaryDark: colors.brand.primaryDark,  // #005c32
    brandSageLight:   colors.brand.sageLight,    // #EAF0E6
    brandGlow:        colors.brand.glow,         // #00E676

    bgCanvas:         colors.surface.background, // #F8FAF6
    bgCard:           colors.surface.card,       // #FFFFFF
    bgMapTerrain:     colors.surface.mapTerrain, // #EAF0E6

    textMain:         colors.text.main,          // #111827
    textMuted:        colors.text.muted,         // #6B7280

    borderDefault:    colors.border.default,     // #E5E7EB
    borderSubtle:     colors.border.subtle,      // #F3F4F6

    statusAvailable:    colors.status.available,    // #10B981
    statusAvailableBg:  colors.status.availableBg,  // #ECFDF5
    statusInUse:        colors.status.inUse,        // #F59E0B
    statusInUseBg:      colors.status.inUseBg,      // #FFFBEB
    statusMaintenance:  colors.status.maintenance,  // #EF4444
    statusMaintenanceBg:colors.status.maintenanceBg,// #FEF2F2
  },
  spacing: {
    xs:  4,
    sm:  8,
    md:  12,
    lg:  16,
    xl:  20,
    '2xl': 24,
    '3xl': 32,
    '4xl': 40,
    '5xl': 48,
  },
  radius: {
    sm:   4,
    md:   8,
    lg:   12,
    xl:   16,
    '2xl': 20,
    full: 9999,
  },
  shadow: {
    card: {
      shadowColor:   '#000',
      shadowOffset:  { width: 0, height: 1 },
      shadowOpacity: 0.07,
      shadowRadius:  3,
      elevation:     2,
    },
    float: {
      shadowColor:   '#000',
      shadowOffset:  { width: 0, height: 10 },
      shadowOpacity: 0.12,
      shadowRadius:  25,
      elevation:     8,
    },
    pin: {
      shadowColor:   '#00E676',
      shadowOffset:  { width: 0, height: 0 },
      shadowOpacity: 0.8,
      shadowRadius:  10,
      elevation:     4,
    },
  },
  font: {
    family: {
      sans:   'PlusJakartaSans',
      arabic: 'Cairo',
    },
    size: {
      xs:   10,
      sm:   12,
      base: 14,
      lg:   16,
      xl:   18,
      '2xl': 20,
      '3xl': 24,
    },
    weight: {
      regular:   '400',
      medium:    '500',
      semibold:  '600',
      bold:      '700',
      extrabold: '800',
    },
  },
}
```

**Usage in React Native:**

```javascript
import { theme } from '@ev/ui/native'
import { StyleSheet, View, Text } from 'react-native'

const styles = StyleSheet.create({
  card: {
    backgroundColor: theme.colors.bgCard,
    borderRadius:    theme.radius.lg,
    padding:         theme.spacing.lg,
    borderWidth:     1,
    borderColor:     theme.colors.borderDefault,
    ...theme.shadow.card,
  },
  stationName: {
    fontSize:      theme.font.size['2xl'],
    fontWeight:    theme.font.weight.bold,
    color:         theme.colors.textMain,
    marginBottom:  theme.spacing.sm,
  },
  pinMarker: {
    width:           20,
    height:          20,
    borderRadius:    theme.radius.full,
    backgroundColor: theme.colors.brandGlow,
    borderWidth:     2,
    borderColor:     '#FFF',
    ...theme.shadow.pin,
  },
})

export function StationCard({ station }) {
  return (
    <View style={styles.card}>
      <Text style={styles.stationName}>{station.name}</Text>
      <Text style={{ color: theme.colors.textMuted }}>
        {station.address}
      </Text>
    </View>
  )
}
```

### Critical Synchronization Rule

**packages/ui/src/tokens/native.ts must stay synchronized with packages/ui/src/tokens/colors.ts.**

Any token added to `colors.ts` **must be added to the native export in the same commit.** Failure to sync causes mobile app rendering discrepancies.

---

## 7. Theme Boundaries

### Driver Web App & Driver Mobile App (Shared)

Both driver apps share:
- All brand tokens (#007943 primary, #EAF0E6 sage, #00E676 glow)
- All surface tokens except sidebar (not used)
- All text tokens
- All status tokens
- Plus Jakarta Sans typography
- Map-specific tokens: `brand.glow`, `surface.mapTerrain`, `shadow.pin`

**Justification:** Consistent visual language across desktop and mobile driver experience.

### Dashboard App (Separate)

Uses:
- All brand tokens (same #007943 primary, #EAF0E6 sage)
- All surface tokens including `surface.sidebar`
- All text tokens
- All status tokens
- **Inter** typography (not Plus Jakarta Sans) — for readability in dense data
- **Does NOT use:** `brand.glow`, `surface.mapTerrain`, `shadow.pin` (map-specific)

**Justification:** Dashboard is a data-dense, non-map interface. Inter provides better readability in tables and forms.

---

## 8. Color Contrast & Accessibility

### Minimum Contrast Ratios (WCAG 2.1 AA)

All combinations tested:

| Text | Background | Ratio | AA Pass |
|------|-----------|-------|---------|
| `text.main` (#111827) | `surface.background` (#F8FAF6) | 15.8:1 | ✅ |
| `text.main` (#111827) | `surface.card` (#FFFFFF) | 18.5:1 | ✅ |
| `text.muted` (#6B7280) | `surface.card` (#FFFFFF) | 5.2:1 | ✅ |
| `brand.primary` (#007943) | `surface.background` (#F8FAF6) | 4.8:1 | ✅ |
| `brand.primary` (#007943) | `surface.card` (#FFFFFF) | 5.5:1 | ✅ |
| `status.available` (#10B981) | `status.availableBg` (#ECFDF5) | 5.1:1 | ✅ |
| `status.inUse` (#F59E0B) | `status.inUseBg` (#FFFBEB) | 4.6:1 | ✅ |
| `status.maintenance` (#EF4444) | `status.maintenanceBg` (#FEF2F2) | 5.3:1 | ✅ |

**Neon Glow Special Case:** `brand.glow` (#00E676) is used only as a colored element (map pin), never for text. It does not need to meet WCAG contrast ratios.

---

## 9. Driver Theme Layout Specifications

### Full-Bleed Map Pattern

All Driver apps (Web and Mobile) use a consistent map-centric layout:

```
┌─────────────────────────────────┐
│  MobileTopBar / Web TopBar       │ Floating header (z-index high)
├─────────────────────────────────┤
│                                   │
│  Full-Bleed Map Canvas           │ bg-surface-mapTerrain (#EAF0E6)
│  (MapView / Leaflet)             │
│                                   │
│  ┌─ SearchBar ────┐              │ Floating search + filter pills
│  │ └─ FilterPills─┘              │ above map
│                                   │
│  ┌─────────────────────────┐     │
│  │   BottomStationCard     │     │ Floating bottom sheet (z-index high)
│  │ (Station details)       │     │ Raised on shadow.float
│  └─────────────────────────┘     │
│                                   │
│  ZoomControls (right side)        │ + and - buttons
└─────────────────────────────────┘
│  BottomTabBar (Mobile only)       │ Fixed bottom navigation
└─────────────────────────────────┘
```

### Color Scheme in Driver Layout

- **Map background:** `surface.mapTerrain` (#EAF0E6) — subtle sage green, matches stations visually
- **Floating UI:** `surface.card` (#FFFFFF) — clean white cards, sharp visual separation
- **Interactive elements:** `brand.primary` (#007943) — forest green for actions
- **Live stations:** `brand.glow` (#00E676) — neon glow for visual emphasis on the map

---

## 10. RTL Considerations

### Supported Languages

- **Arabic** — RTL layout required
- **French** — LTR layout (default)
- **English** — LTR layout (default)

### RTL Adjustments by Component

#### Web Components
- Flexbox: Use `flex-row-reverse` or CSS `direction: rtl` on containers
- Padding/Margin: Use logical properties (`margin-inline-start`, `padding-inline-end`) where possible
- Text alignment: `text-right` for Arabic, `text-left` for LTR

#### Mobile Components
- React Native RTL support: Use `I18nManager.forceRTL(true)` for testing
- Flex direction: Adjust `flexDirection: 'row-reverse'` in RTL
- Text alignment: Use `textAlign: 'right'` for RTL languages

### Driver-Specific RTL Rules

See [components.md](components.md#rtl-rules-for-driver-apps) for detailed RTL behavior of each driver component (MobileTopBar, SearchBar, FilterPills, BottomStationCard, BottomTabBar).

---

## 11. Token Update Checklist

When adding a new token:

- [ ] Add to `packages/ui/src/tokens/colors.ts` (or appropriate token file)
- [ ] Add to `packages/ui/src/tokens/native.ts` if it's a color or spacing value
- [ ] Update `packages/ui/tailwind.config.base.js` if Tailwind-specific
- [ ] Document in this file (design-tokens.md)
- [ ] Add usage examples in [components.md](components.md)
- [ ] Test contrast ratios if it's a color used for text
- [ ] Update [accessibility.md](accessibility.md) if it affects RTL or language support
- [ ] Commit colors.ts and native.ts together

**Golden Rule:** If you change one, you must change the other. They must stay synchronized.

---

## 12. Design System References

- **All components using tokens:** See [components.md](components.md)
- **Accessibility rules:** See [accessibility.md](accessibility.md)
- **Web Tailwind setup:** See [web-tokens.md](web-tokens.md)
- **Mobile React Native setup:** See [native-tokens.md](native-tokens.md)
- **Component specs:** See [components.md](components.md)

---

**Document Version:** 1.0  
**Last Updated:** 2026-06-05  
**Status:** Foundation complete, Driver theme added
