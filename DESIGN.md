---
name: BorneMap
description: EV charging station discovery and management platform for Tunisia
colors:
  pine: "#007943"
  pine-deep: "#166534"
  moss-tint: "#EAF0E6"
  ink: "#1f2937"
  ink-muted: "#6b7280"
  ink-subtle: "#9ca3af"
  surface: "#ffffff"
  surface-muted: "#f3f4f6"
  border: "#d1d5db"
  border-subtle: "#e5e7eb"
  error: "#dc2626"
  error-surface: "#fef2f2"
  success: "#16a34a"
typography:
  display:
    fontFamily: "system-ui, -apple-system, sans-serif"
    fontSize: "clamp(1.5rem, 4vw, 2rem)"
    fontWeight: 700
    lineHeight: 1.2
  title:
    fontFamily: "system-ui, -apple-system, sans-serif"
    fontSize: "clamp(1rem, 2.5vw, 1.25rem)"
    fontWeight: 600
    lineHeight: 1.4
  body:
    fontFamily: "system-ui, -apple-system, sans-serif"
    fontSize: "0.875rem"
    fontWeight: 400
    lineHeight: 1.5
  label:
    fontFamily: "system-ui, -apple-system, sans-serif"
    fontSize: "0.75rem"
    fontWeight: 500
    lineHeight: 1.25
    letterSpacing: "0.025em"
rounded:
  sm: "4px"
  md: "6px"
  lg: "8px"
spacing:
  xs: "4px"
  sm: "8px"
  md: "16px"
  lg: "24px"
  xl: "32px"
components:
  nav-item:
    backgroundColor: "transparent"
    textColor: "{colors.ink-muted}"
    rounded: "{rounded.md}"
    padding: "8px 12px"
  nav-item-active:
    backgroundColor: "{colors.moss-tint}"
    textColor: "{colors.pine}"
    rounded: "{rounded.md}"
    padding: "8px 12px"
  stat-card:
    backgroundColor: "{colors.surface}"
    rounded: "{rounded.lg}"
    padding: "20px 24px"
  error-banner:
    backgroundColor: "{colors.error-surface}"
    textColor: "{colors.error}"
    rounded: "{rounded.md}"
    padding: "8px 16px"
  loading-indicator:
    backgroundColor: "{colors.surface}"
    rounded: "{rounded.md}"
    padding: "8px 16px"
---

# Design System: BorneMap

## 1. Overview

**Creative North Star: "The Quiet Dashboard"**

BorneMap's interfaces feel like a calm, reliable instrument — precise without being cold, present without being loud. The design steps back so the data leads. There is no decorative flourish that doesn't serve the user's task.

This system explicitly rejects the generic corporate dashboard aesthetic: overdesigned admin panels, dark-themed SaaS interiors, heavy cards-and-gradients. Instead, it is **flat by default**, using tonal layering rather than shadows to create hierarchy. Surfaces sit quietly; the information on them is what catches your eye.

**Key Characteristics:**
- Flat, tonal hierarchy — no drop shadows at rest
- Earthy green accent (Pine) used sparingly — it signals active state, never decoration
- Refined and restrained components — clean lines, generous padding, gentle rounding (6-8px)
- System font stack — no custom typefaces; clarity over personality
- Calm colors, calm spacing, calm motion — nothing competes for attention

## 2. Colors: The Pine & Moss Palette

The palette is built around earthy, natural greens — trustworthy and grounded, like a forest path. The warm olive neutrals keep the system from feeling cold or infrastructural.

### Primary
- **Pine** (#007943 / oklch(0.52 0.13 150)): Primary interactive color. Active nav items, buttons, links, badges. Used sparingly — its rarity is the point.
- **Deep Pine** (#166534 / oklch(0.44 0.10 150)): Hover and emphasis states. Stronger visual weight for pressed buttons or prominent counts.
- **Moss Tint** (#EAF0E6 / oklch(0.93 0.02 140)): Subtle surface tint. Active background for nav items, subtle callouts, badge backgrounds.

### Neutral
- **Ink** (#1f2937): Body text and primary headings.
- **Ink Muted** (#6b7280): Secondary text, metadata, labels.
- **Ink Subtle** (#9ca3af): Placeholder text, disabled content.
- **Surface** (#ffffff): Card and container backgrounds.
- **Surface Muted** (#f3f4f6): Page background, subtle section separation.
- **Border** (#d1d5db): Standard dividers and container strokes.
- **Border Subtle** (#e5e7eb): Light dividers, subdued borders.

### Semantic
- **Error** (#dc2626): Error text and icon. Always paired with a descriptive message.
- **Error Surface** (#fef2f2): Error banner background.
- **Success** (#16a34a): Positive indicators (chargers available, confirmed actions).

### Named Rules

**The Quiet Accent Rule.** Pine (the primary green) is used on 10% or less of any given screen. Most of the interface is neutral. Color draws attention to the active thing and nothing else.

**The Status-Last Rule.** Semantic colors (error, success) always appear after the neutral information they qualify, never before. A station name and address come first; availability color follows. Information hierarchy first, emotional signal second.

## 3. Typography

**Body Font:** System UI stack (system-ui, -apple-system, sans-serif)

The Tailwind default system stack is fast, familiar, and entirely neutral. For a quiet product interface, custom typefaces would add personality without purpose. Every user's own system type is already the most readable option for their device.

### Hierarchy
- **Display** (700, clamp(1.5rem, 4vw, 2rem), 1.2): Page titles. Dashboard section headings ("Overview", "Partners"). Appears once per page.
- **Title** (600, clamp(1rem, 2.5vw, 1.25rem), 1.4): Card headings, sidebar app name, subsection titles.
- **Body** (400, 0.875rem / 14px, 1.5): Primary reading text. Station names, addresses, descriptions. Max line length 65ch.
- **Label** (500, 0.75rem / 12px, 1.25, 0.025em letter-spacing): Metadata, stat card labels, sidebar nav items, badge text. Uppercase reserved for badges only (max 4 words).

### Named Rules

**The One-Page Ceiling Rule.** Display type appears exactly once per page. It names the current surface. Everything below it uses Title or Body.

## 4. Elevation

Flat by default. Hierarchy is conveyed through tonal layering (surface → surface-muted → border) rather than drop shadows. This keeps the interface quiet and prevents visual noise.

The only exception is the map loading badge, which uses a single lightweight shadow (`0 1px 3px rgba(0,0,0,0.1)`) to float above the map canvas — a functional necessity, not a decorative choice.

**No shadow vocabulary is defined.** Shadows are not part of the system's visual language. If a surface needs separation from another, use a border or a background tint.

### Named Rules

**The Flat-By-Default Rule.** Surfaces are flat at rest. No box-shadow on cards, panels, sidebars, or modals. Depth is communicated through tonal contrast, not simulated elevation.

## 5. Components

### Sidebar Navigation
- **Shape:** Gentle rounding on active indicator (rounded-md, 6px). Full-height bar is flat.
- **Default:** Transparent background, muted ink text (Ink Muted / #6b7280).
- **Active:** Moss Tint (#EAF0E6) background with Pine (#007943) text. No icon, no border stripe — the color change alone signals the active section.
- **Hover:** Gray-50 background, Ink (#1f2937) text.
- **Padding:** 8px vertical, 12px horizontal per item.

### Stat Cards (Dashboard)
- **Shape:** Rounded corners (rounded-lg, 8px). Light border (Border Subtle / #e5e7eb) defines the container boundary.
- **Background:** White (Surface / #ffffff).
- **Shadow:** None. The card sits flat on the muted page background.
- **Label:** Label type (12px, 500 weight, Ink Muted).
- **Value:** Display type (clamp scale, 700 weight, Ink).
- **Grid:** Cards sit in a responsive grid (`repeat(auto-fit, minmax(280px, 1fr))`), gap 24px.

### Error Banner
- **Shape:** Rounded (rounded-md, 6px). No shadow.
- **Background:** Error Surface (#fef2f2).
- **Text:** Error (#dc2626), Body size (14px).
- **Border:** None. The tint alone signals the state.

### Loading Indicator
- **Shape:** Rounded (rounded-md, 6px).
- **Background:** White, floating above content. No shadow in page context; lightweight shadow (`0 1px 3px rgba(0,0,0,0.1)`) only when overlaying a map.
- **Text:** Body size, Ink Muted. Prefix description ("Loading..."), never a spinner alone.

### Map Markers (Driver Web / Mobile)
- Marker icons use Leaflet defaults (driver web) or react-native-maps defaults (mobile) — intentionally not customized. The map's visual language comes from the tile layer and the popup cards, not custom pins.
- **Popup card (web):** Body type stack. Station name in Title weight. Address in Body weight, Ink Muted. Charger count in Success (#16a34a). Distance in Ink Subtle. No custom popup chrome — Leaflet default.

## 6. Do's and Don'ts

### Do:
- **Do** use Pine (#007943) as the single accent color. One green, one role: active state.
- **Do** keep surfaces flat. Use tonal layering (border + background tint) for hierarchy, never box-shadow on cards or panels.
- **Do** put information first, status second. Station name and address before charger count.
- **Do** use the system font stack for all text. No imported web fonts — clarity and speed over typographic personality.
- **Do** use gentle rounding: 6px for nav items and inputs, 8px for cards. Nothing more rounded than 8px.
- **Do** space generously. 24px between cards, 16px internal padding as the default unit.

### Don't:
- **Don't** apply shadows to cards, panels, or sidebars. No `shadow-sm`, `shadow-md`, or any box-shadow on resting surfaces.
- **Don't** use Pine as a decorative background. It is for interactive or active states only.
- **Don't** use the green accent on more than 10% of any given screen. If the page looks green, the accent has lost its signal.
- **Don't** build generic corporate dashboard patterns: no dark sidebars, no glowing gradients on stat cards, no icon-filled nav with tooltip labels, no border-left colored stripes on list items.
- **Don't** use `border-radius` larger than 8px on any container, card, or section. Reserve pill shapes for badges only (and badges should be rare).
- **Don't** use a spinner alone for loading. Always pair with a text description of what is loading.
- **Don't** uppercase body text. Uppercase is for badges (max 4 words) only.
- **Don't** nest cards. If a surface has multiple information groups, separate them with spacing and borders, not nested containers.
