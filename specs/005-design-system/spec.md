# Feature Specification: Design System — UI Primitives & Tokens

**Feature Branch**: `005-design-system`

**Created**: 2026-06-11

**Status**: Draft

**Input**: Sprint 1.4 — Design System (Critical Path Blocker) per docs/mvp/mvp-1-discovery.md

## User Scenarios & Testing

### User Story 1 — Design Tokens (Priority: P1)

A developer building screens for the Borne mobile app needs a centralized set of design tokens so that all screens share consistent colors (light/dark), spacing, typography, shadows, and radii without hardcoding any style values.

**Why this priority**: Design tokens are the foundation of the entire design system. Every component and screen depends on them. Without tokens, every UI element would use hardcoded values, making theming and maintenance impossible.

**Independent Test**: Import tokens into a test screen, apply each token category (color, spacing, typography, shadow, radii), and verify the rendered output uses the token values — confirmed by visual diff or snapshot. Toggle between light and dark mode and verify all color tokens switch correctly.

**Acceptance Scenarios**:

1. **Given** a React Native project with the design tokens module, **When** a developer imports color tokens and applies them to a view, **Then** the view renders with the correct color value from the token
2. **Given** the device is in light mode, **When** the app renders any component using color tokens, **Then** light-mode color values are displayed
3. **Given** the device switches to dark mode, **When** the app re-renders, **Then** dark-mode color values are applied automatically
4. **Given** a developer inspects the codebase, **When** they search for color/spacing/typography values, **Then** no hardcoded style values exist — all values come from tokens

---

### User Story 2 — Button Component (Priority: P1)

A user tapping a call-to-action on the map or bottom sheet expects visual and haptic feedback. The button must support primary action style and integrate with the device's haptic engine.

**Why this priority**: Buttons are the primary interaction mechanism. Every screen needs them for user actions (e.g., "Navigate", "Retry", "Confirm").

**Independent Test**: Render the button component in a test harness, verify the ripple/scale animation on press, verify the haptic fires on a physical device, and confirm the component accepts all required props (label, onPress, variant).

**Acceptance Scenarios**:

1. **Given** a primary CTA button on screen, **When** the user taps it, **Then** the button shows a press animation (scale/ripple) and fires a haptic feedback
2. **Given** a button with a disabled prop, **When** rendered, **Then** it appears visually dimmed and does not respond to taps
3. **Given** a button with a loading prop, **When** rendered, **Then** it shows a spinner in place of the label and ignores taps

---

### User Story 3 — Skeleton Loader (Priority: P1)

A user opens the map or station list and sees loading content. Skeleton placeholders render immediately so the user perceives instant feedback while data loads.

**Why this priority**: The MVP UX rule requires skeleton-first loading on every screen. No blank states are permitted during loading.

**Independent Test**: Render the skeleton component without data and verify it displays animated placeholder shapes matching the expected content layout (map skeleton: full-screen rectangle with shimmer; list skeleton: rows of rectangles with shimmer).

**Acceptance Scenarios**:

1. **Given** the map screen is loading station data, **When** the data fetch is in progress, **Then** a full-screen map skeleton with animated shimmer is displayed
2. **Given** a list of stations is loading, **When** the fetch is in progress, **Then** a list skeleton (3-5 rows with avatar + text placeholders) with animated shimmer is displayed
3. **Given** data loading completes, **When** the real content arrives, **Then** the skeleton fades out and the content replaces it smoothly

---

### User Story 4 — Empty State (Priority: P2)

A user opens the map or searches for nearby stations but no results are available. The app shows a clear empty state explaining the situation and suggesting next steps.

**Why this priority**: Empty states prevent user confusion when no data is available. Tied with error states for UX completeness.

**Independent Test**: Render the empty state component with different messages (no stations nearby, GPS unavailable) and verify the appropriate illustration, message, and optional CTA are displayed.

**Acceptance Scenarios**:

1. **Given** the user is in an area with no nearby stations, **When** the nearby search returns zero results, **Then** an empty state with "No stations nearby" message and an illustration is shown
2. **Given** GPS is unavailable, **When** the app cannot determine location, **Then** an empty state with "Enable GPS to find stations" message and an illustration is shown

---

### User Story 5 — Error State (Priority: P2)

A network error occurs while fetching stations or events. The app shows a friendly error state with a retry CTA so the user can recover without restarting the app.

**Why this priority**: Error states are essential for graceful failure handling. The MVP mandates graceful error handling on every screen.

**Independent Test**: Render the error state component with a mock error message, verify the retry button is displayed, and verify tapping retry triggers the onRetry callback.

**Acceptance Scenarios**:

1. **Given** a network request fails, **When** the error is caught, **Then** an error state with a descriptive message and a "Retry" CTA button is displayed
2. **Given** the error state is visible, **When** the user taps "Retry", **Then** the onRetry callback is invoked and the loading state resumes

---

### User Story 6 — Bottom Sheet (Priority: P2)

A user taps a station marker on the map. A bottom sheet slides up with station details. The sheet must animate smoothly with Reanimated v3 and be reusable for other content types.

**Why this priority**: The bottom sheet is the primary UI for station details — the core interaction after map exploration. It must be built as a reusable primitive.

**Independent Test**: Render the bottom sheet with sample content, verify it animates open/close with Reanimated v3, verify it fills 60-80% of the screen height, and verify it can be dismissed by swiping down.

**Acceptance Scenarios**:

1. **Given** a user taps a station marker, **When** the marker is selected, **Then** a bottom sheet slides up smoothly (Reanimated v3 spring animation) showing station details
2. **Given** the bottom sheet is open, **When** the user swipes down, **Then** the sheet follows the gesture and snaps closed
3. **Given** the bottom sheet contains scrollable content, **When** the user scrolls, **Then** the sheet does not dismiss — only a swipe on the handle area triggers dismissal

---

### Edge Cases

- What happens when dark mode is toggled rapidly? → All tokens update within a single frame; no flicker or delay between theme switches
- What happens when the skeleton animation runs for an extended period (e.g., slow network)? → Shimmer animation loops continuously without jank; no timeout or abrupt stop
- What happens when both error and empty conditions are true? → Error state takes precedence over empty state
- What happens when the bottom sheet content exceeds the viewport? → Content scrolls within the sheet; the sheet handle remains visible for dismissal

## Requirements

### Functional Requirements

- **FR-001**: System MUST provide a `tokens.ts` module exporting color tokens for light and dark modes (primary, secondary, background, surface, text, error, success)
- **FR-002**: System MUST provide a `tokens.ts` module exporting spacing scale (4px base increments: 4, 8, 12, 16, 20, 24, 32, 48, 64)
- **FR-003**: System MUST provide a `tokens.ts` module exporting typography scale (font families, font sizes, font weights, line heights)
- **FR-004**: System MUST provide a `tokens.ts` module exporting radii scale (none, sm, md, lg, full)
- **FR-005**: System MUST provide a `tokens.ts` module exporting shadow presets (elevation levels for cards, sheets, modals)
- **FR-006**: System MUST NOT use hardcoded style values — all colors, spacing, typography, shadows, and radii MUST reference design tokens
- **FR-007**: System MUST provide a Button component supporting variants (primary, secondary, ghost), states (default, disabled, loading), and haptic feedback on press
- **FR-008**: System MUST provide a Skeleton component supporting map layout (full-screen rectangle) and list layout (rows with avatar + text placeholders), with animated shimmer
- **FR-009**: System MUST provide an EmptyState component accepting customizable title, description, illustration, and optional action CTA
- **FR-010**: System MUST provide an ErrorState component accepting error message and retry CTA, with onRetry callback
- **FR-011**: System MUST provide a BottomSheet component built with Reanimated v3, supporting: spring animation open/close, gesture-driven swipe-to-dismiss, scrollable content, and configurable snap points

### Key Entities

- **Design Token**: A named style value (color, spacing, typography size, radii, shadow) exported from `tokens.ts`. Every token has a light and dark variant where applicable (colors). Tokens are consumed by all UI components.
- **Button**: A pressable UI primitive with visual feedback (scale animation) and haptic response. Variants: primary, secondary, ghost. States: default, disabled, loading.
- **Skeleton**: An animated placeholder shape that renders while content loads. Layouts: map (full-screen rectangle), list (rows). Animation: shimmer sweep.
- **EmptyState**: A composite component with illustration, title, description, and optional CTA button. Use cases: no stations nearby, GPS unavailable.
- **ErrorState**: A composite component with error icon, descriptive message, and retry button. Invokes onRetry callback on tap.
- **BottomSheet**: A draggable panel that slides up from the bottom of the screen. Built with Reanimated v3 shared animation APIs. Dismissible by swipe-down gesture. Supports scrollable content and configurable snap points.

## Success Criteria

### Measurable Outcomes

- **SC-001**: All UI components in the design system can be rendered in a test harness (Storybook or similar) within a single command — 100% of components render without errors
- **SC-002**: Dark mode toggle applies all color token changes within a single frame — no flicker, no delay, no hardcoded color values detected by audit
- **SC-003**: Skeleton components appear within 100ms of screen mount and animate smoothly at 60fps
- **SC-004**: Bottom sheet animation completes in under 300ms (spring settle time) on a mid-range device
- **SC-005**: Button press provides visual feedback (scale animation) within 50ms and haptic fires within 100ms on physical device

## Assumptions

- The design system targets React Native (Expo SDK 54) with TypeScript
- Animation runtime is Reanimated v3 (mandated by MVP UX rules)
- Dark mode is detected via Appearance API or system preference; no manual toggle needed in MVP-1
- Haptic feedback uses `expo-haptics` (already present in the Expo ecosystem)
- Design tokens are plain TypeScript objects consumed via direct import and the ThemeProvider context hook
- Components are developed in isolation first (Storybook or similar tool), then integrated into screens
- The bottom sheet is consumed by the Station Detail screen (Sprint 1.5) but implemented here as a reusable primitive
- SVG illustrations for empty states are provided by the designer separately
