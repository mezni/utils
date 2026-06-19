# Guardrail — UX/UI Base

Applies to: `apps/mobile-driver`, `apps/web-driver`, `apps/dashboard`

---

## Four-state contract (always enforced)

Every API-interacting screen MUST implement exactly four states:

| State | Rendering | Details |
|-------|-----------|---------|
| **Loading** | Shimmer skeleton | Must mirror the shape of the target card/list layout. No spinners, no blank screens. |
| **Success** | Animated layout | Framer Motion (web) or LayoutAnimation (React Native). Smooth transition from skeleton to content. |
| **Empty** | Illustrative feedback | Guide the user: "Pan to Tunis, Sousse, or Sfax to discover charging stations." Never a bare "No results." |
| **Error** | Error boundary | Prominent "Retry Connection" button + message. Catch all errors at the boundary level, not per-component. |

Failure to implement any state is a Tier 2 review violation (see `code-review.md`).

---

## Map interaction rules

- **Viewport debounce**: Minimum 300ms before firing `/api/v1/nearby`. Debounce is on `moveend`/`regionchange` events — not on every pixel change.
- **Zoom threshold**: When zoomed out past a defined level (map zoom < 12), hide all station markers and overlay a message: "Zoom in closer to view available charging stations".
- **Marker clustering**: For zoom levels 12-14, cluster nearby stations. Above zoom 14, show individual markers.
- **Web (Leaflet)**: SVGs and marker PNGs MUST be bundled locally and pre-loaded. Do not fetch from external CDNs.
- **Mobile (React Native Maps)**: Animated markers for station state transitions (available → in-use → offline).

---

## Mobile-specific rules (`mobile-driver`)

- **Zero native modules**: Must run in default Expo Go. No custom native modules, no `expo-dev-client`, no native config plugins that require `npx expo run`.
- **Offline cache**: On successful `/api/v1/nearby` response, save the coordinate/location snapshot to `AsyncStorage`. On network failure, read `AsyncStorage` cache, render cached markers, and display a banner: "Viewing cached data — last updated [timestamp]".
- **Touch targets**: All interactive elements ≥44x44pt. Use `hitSlop` for smaller elements.
- **Safe areas**: Respect notches, status bars, and home indicators via `react-native-safe-area-context`.
- **Navigation**: Bottom tab navigation with ≤5 tabs. Stack navigation within each tab.
- **Back gesture**: Swipe-back gesture must work on all stack screens. Do not disable it.

---

## Web-specific rules (`web-driver`)

- **Tailwind only**: All styling via the shared Tailwind config in `packages/shared-ui`. No CSS modules, no styled-components.
- **Responsive**: Map must be usable at 375px (small mobile) and 1440px (desktop).
- **Leaflet**: Use React Leaflet bindings. Markers bundled as local SVGs. No CDN dependencies.

---

## Dashboard-specific rules (`dashboard`)

- **Framer Motion**: Limited to route transitions only. Do not use Framer Motion for per-element micro-interactions (use CSS transitions instead).
- **shadcn/ui**: Use shadcn/ui components for all standard UI patterns (buttons, forms, tables, dialogs, toasts).
- **React Router v6**: All routes must use `createBrowserRouter` with loaders for data fetching.
- **React Query**: All server state goes through React Query. No `useEffect` for data fetching.
- **Form validation**: Use React Hook Form + Zod for all forms. Client-side validation before submit, server-side validation feedback after.

---

## Accessibility (all platforms)

- Color contrast: body text ≥4.5:1, large text ≥3:1 against background.
- Focus states visible on all interactive elements (web: `:focus-visible`, mobile: `accessibilityState`).
- Screen reader labels on all meaningful icons (`aria-label` / `accessibilityLabel`).
- Touch/click feedback within 100ms of interaction.
- Reduced motion respected: prefer `transform` + `opacity` animations (GPU-composited). Use `prefers-reduced-motion` / `AccessibilityInfo.isReduceMotionEnabled`.

---

## Form patterns (all platforms)

- Every form field has inline validation feedback (error message below the field, not in a toast).
- Submit buttons show loading state (`disabled` + spinner) while the request is in flight.
- Error recovery: error messages must tell the user what to fix, not just "Invalid input".
- Progressive disclosure: long forms split into steps with a progress indicator.

---

## Visual consistency

- Color palette: defined in `packages/shared-ui/tailwind.config.ts`. No hardcoded hex values.
- Typography: font scale and weights defined in shared Tailwind config. No inline `font-size` or `font-weight`.
- Spacing: use the 4px grid system from Tailwind. No arbitrary margins/padding.
- Icons: use Lucide (web) and Phosphor (React Native). No emojis as icons.

---

## Self-check before submitting

- [ ] All four states (loading, success, empty, error) implemented for every API-interacting screen
- [ ] Shimmer skeleton matches the target content shape
- [ ] Map viewport uses ≥300ms debounce
- [ ] Map hides markers + shows overlay when zoomed past threshold
- [ ] Mobile: runs in Expo Go with zero native modules
- [ ] Mobile: offline cache updated on successful queries
- [ ] Web: all styling via shared Tailwind config
- [ ] Dashboard: Framer Motion used for route transitions only
- [ ] All touch targets ≥44x44pt
- [ ] Forms use Zod + React Hook Form validation
- [ ] No hardcoded colors, font sizes, or spacing — use tokens
