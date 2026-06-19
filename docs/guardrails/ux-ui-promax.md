# Guardrail — UX/UI Pro Max

Applies to: `apps/mobile-driver`, `apps/dashboard`

Extends `guardrails/ux-ui.md`. When a conflict exists between this file and `ux-ui.md`, this file wins.

---

## Design system — token completeness

Every UI component must reference the design system tokens defined in `packages/shared-ui/`. No inline values.

```typescript
// ✅ Correct: uses tokens
<Button className="bg-primary text-primary-foreground" />

// ❌ Wrong: hardcoded value
<Button className="bg-blue-600 text-white" />
```

---

## Animation & motion guidelines

### Framer Motion (dashboard)

Route transitions only — page-level `AnimatePresence` wrapping `<Outlet />`:

```tsx
<AnimatePresence mode="wait">
  <Outlet />
</AnimatePresence>
```

Page enter/exit: fade + subtle vertical slide (8px, 300ms, ease-in-out).
No element-level entrance animations except for list `layoutId` transitions.

### LayoutAnimation (React Native)

Use `LayoutAnimation.configureNext()` for list insertions/removals and state-driven layout changes.

```tsx
LayoutAnimation.configureNext(LayoutAnimation.Presets.easeInEaseOut);
setStations(newStations);
```

### Performance rules

- Animate only `transform` and `opacity` — never `width`, `height`, `top`, `left` (triggers layout recalc).
- Animation duration: 150-300ms for micro-interactions, 300-500ms for page transitions.
- Respect `prefers-reduced-motion`: skip all non-essential animations when detected.

---

## Map component performance

### React Native Maps (mobile-driver)

- Use `AnimatedRegion` for marker position updates — never re-render markers on every coordinate change.
- Batch marker updates: collect all coordinate changes, apply in a single `setState`.
- Marker clustering: use `react-native-map-clustering` or manual clustering at zoom levels 12-14.
- Debounce `onRegionChangeComplete` by 300ms before calling `/api/v1/nearby`.

### React Leaflet (web-driver)

- `useMemo` on marker arrays to prevent unnecessary re-renders.
- `React.memo` on `StationMarker` component — compare by `station.id` only.
- Pre-load map tiles: specify `TileLayer` with `maxZoom` and `minZoom` bounds.

---

## Loading state — shimmer skeletons

Each screen type has a specific shimmer pattern:

| Screen | Shimmer shape | Implementation |
|--------|--------------|---------------|
| Map with station list | Card stack (3-4 cards) with placeholder text lines | `StationCardSkeleton` component |
| Station detail page | Image block + 4 text lines | `StationDetailSkeleton` component |
| Dashboard stats grid | 4-6 rectangular blocks | `StatCardSkeleton` component |
| Dashboard table | 5-8 table rows with varying column widths | `TableSkeleton` component |
| Form page | 4-6 form field placeholders | `FormSkeleton` component |

```tsx
// ✅ Correct: skeleton mirrors the target card layout
function StationCardSkeleton() {
  return (
    <div className="flex gap-3 p-4 rounded-lg bg-muted animate-pulse">
      <div className="w-16 h-16 rounded-md bg-muted-foreground/20" />
      <div className="flex-1 space-y-2">
        <div className="h-4 w-3/4 rounded bg-muted-foreground/20" />
        <div className="h-3 w-1/2 rounded bg-muted-foreground/20" />
        <div className="h-3 w-1/3 rounded bg-muted-foreground/20" />
      </div>
    </div>
  );
}
```

---

## Empty state — illustrative guidance

Empty states must be contextual, never generic:

| Context | Empty message | Illustration |
|---------|--------------|-------------|
| Nearby stations (map view) | "No charging stations in this area. Try panning to Tunis, Sousse, or Sfax." | Map outline with location pins |
| Favorites (empty list) | "Save your favourite charging stations for quick access." | Empty heart with arrow pointing to a station pin |
| Reviews (no reviews) | "No reviews yet. Be the first to review this station!" | Speech bubble with star |
| Partner dashboard (no stations) | "You haven't added any charging stations yet. Click 'Add Station' to get started." | Plug icon with plus |
| Search results (no match) | "No stations match your search. Try different keywords." | Search icon with question mark |

Rules:
- Every empty state has an SVG illustration (not an emoji, not a text-only message).
- Empty state takes the full card/list area — not a small inline message.
- CTA button or actionable link in every empty state.

---

## Error boundary & offline handling

- Wrap every screen in an error boundary. Catch at the screen level, not per-component.
- Error state: prominent "Retry Connection" button + descriptive message.
- Offline detection: listen to `NetInfo` (React Native) / `navigator.onLine` (web).
- Offline banner: "You are offline. Showing cached data from [timestamp]." Fixed at top of screen, non-dismissable until back online.
- On reconnection: auto-refresh stale data, dismiss banner with a slide-up animation.

---

## Platform-specific patterns (React Native)

### List virtualization
- Use `FlatList` with `getItemLayout` for fixed-height items.
- `windowSize` prop: set to 5 for station lists, 10 for short lists.
- Never use `ScrollView` for lists with >20 items — only `FlatList` or `SectionList`.

### Image handling
- Use `expo-image` (not `Image` from React Native) for cached remote images.
- Placeholder background color matching the dominant image color while loading.

### Keyboard handling
- Use `KeyboardAvoidingView` on form screens.
- `scrollToInput()` on input focus for forms with many fields.
- Dismiss keyboard on scroll for list screens.

---

## Color token enforcement

| Token | Usage | Example value |
|-------|-------|--------------|
| `primary` | Main CTAs, active states, selected markers | BorneMap brand color |
| `primary-foreground` | Text on primary background | White |
| `secondary` | Secondary CTAs, filter chips | Muted brand tint |
| `accent` | Highlights, badges, notification dots | From design system |
| `destructive` | Delete, remove, irreversible actions | Red tone |
| `muted` | Card backgrounds, disabled states | Light gray |
| `muted-foreground` | Secondary text, hints, metadata | Gray |
| `ring` | Focus indicators, selected borders | Primary color |
| `success` | Online, available, confirmed statuses | Green |
| `warning` | Maintenance, limited availability | Amber |
| `error` | Offline, error, failed statuses | Red |

---

## Self-check before submitting

- [ ] No hardcoded colors, spacing, or typography values
- [ ] Framer Motion used only for route transitions (dashboard)
- [ ] Shimmer skeleton matches target content shape exactly
- [ ] Empty state has contextual SVG illustration + CTA
- [ ] Error boundary at screen level with Retry Connection
- [ ] Offline banner with cached data timestamp
- [ ] FlatList with `getItemLayout` for lists >20 items
- [ ] Animation: only `transform` + `opacity`, 150-300ms
- [ ] Map markers: `AnimatedRegion` (RN) / `React.memo` (web)
- [ ] Respects reduced motion preference
- [ ] All touch targets ≥44x44pt
