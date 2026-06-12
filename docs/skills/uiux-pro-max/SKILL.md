# UX Pro Max Standard

## Overview

This skill defines the highest achievable UX quality for the BorneMap mobile driver app. Every interaction is deliberate, every transition is smooth, every state is designed.

**Constitutional Enforcer:** This skill provides the enforcement mechanism for Section 7 of the BorneMap Constitution (UX MANDATE).

---

## Core Principles

### 1. Skeleton Screens Over Spinners
- **Rule:** Never use native spinners or loading circles
- **Implementation:** Use `SkeletonBox`, `SkeletonGroup`, and screen-specific skeleton components
- **Animation:** Smooth fade-in/out transitions, no jitter
- **Example:**
  ```typescript
  // Instead of:
  <ActivityIndicator size="large" color="#3b82f6" />

  // Use:
  <StationListItemSkeleton />
  ```

### 2. Optimistic UI on User Actions
- **Rule:** Immediately update UI before backend confirmation
- **Implementation:** Update store state, then show optimistic feedback
- **Rollback:** Show error state if backend fails
- **Example:**
  ```typescript
  // FAVORITE button
  const toggleFavorite = async (stationId: string) => {
    // Optimistic update
    setFavorites(prev => [...prev, stationId]);

    try {
      await api.favorite(stationId);
      showToast('Added to favorites');
    } catch (error) {
      // Rollback
      setFavorites(prev => prev.filter(id => id !== stationId));
      showErrorToast('Failed to add to favorites');
    }
  };
  ```

### 3. Haptic Feedback on Primary CTAs
- **Rule:** Every primary action triggers haptics
- **Implementation:** Use `expo-haptics.impactAsync()`
- **Types:**
  - `ImpactFeedbackStyle.Light` — micro-interactions
  - `ImpactFeedbackStyle.Medium` — standard actions
  - `ImpactFeedbackStyle.Heavy` — destructive actions
  - `ImpactFeedbackStyle.Rigid` — buttons, toggles
- **Example:**
  ```typescript
  import * as Haptics from 'expo-haptics';

  const handleConfirm = async () => {
    Haptics.impactAsync(Haptics.ImpactFeedbackStyle.Rigid);
    await api.bookStation(stationId);
  };
  ```

### 4. Gesture-First Design
- **Rule:** Every flow should have swipe/drag gestures
- **Common Patterns:**
  - **Bottom Sheet:** Swipe-to-dismiss, drag handle
  - **List Items:** Swipe actions (favorite, navigate)
  - **Detail Views:** Pull-to-refresh, swipe-back
  - **Modals:** Swipe-to-dismiss gesture
- **Implementation:** Use `react-native-reanimated` gestures

### 5. Empty States Fully Designed
- **Rule:** No blank screens, ever
- **Implementation:** Custom `EmptyState` component with:
  - Illustration/icon
  - Clear message
  - Recovery action button
  - Divider to content
- **Example:**
  ```typescript
  <EmptyState
    icon="map-marker-slash"
    message="No stations found"
    submessage="Try increasing the search radius"
    action="resetFilters"
  />
  ```

### 6. Error States with Recovery Actions
- **Rule:** Never show raw error strings
- **Implementation:** `ErrorState` component with:
  - Friendly error message
  - Recovery action button
  - Optional "retry" toggle
  - Copy error details option (for developers)
- **Example:**
  ```typescript
  <ErrorState
    message="Failed to load stations"
    submessage="Check your connection and try again"
    action="retry"
    actionLabel="Retry"
  />
  ```

### 7. Dark Mode on Every Screen
- **Rule:** Both light and dark themes supported from day one
- **Implementation:**
  - Design tokens use `colors.dark` and `colors.light` variants
  - Theme provider wraps app
  - Toggle accessible from settings
- **Example:**
  ```typescript
  // tokens.ts
  export const colors = {
    light: {
      background: '#ffffff',
      text: '#000000',
      surface: '#f3f4f6'
    },
    dark: {
      background: '#000000',
      text: '#ffffff',
      surface: '#1a1a1a'
    }
  };

  // component.tsx
  <View style={{ backgroundColor: tokens.colors.background }}>
    <Text style={{ color: tokens.colors.text }}>
      Station Name
    </Text>
  </View>
  ```

### 8. No Map Jitter
- **Rule:** Map interactions must not cause marker flashing or unnecessary re-renders
- **Implementation:**
  - Memoize markers with unique IDs
  - Use `react-native-maps` optimized markers
  - Debounce proximity queries
  - Only re-render changed markers
- **Example:**
  ```typescript
  const markers = useMemo(() =>
    stations.map(station => ({
      key: station.id,
      coordinate: {
        latitude: station.lat,
        longitude: station.lng
      },
      title: station.name,
      description: `${station.charger_count} chargers`
    })),
    [stations]
  );
  ```

### 9. Animation Quality
- **Rule:** Use `react-native-reanimated` v3 for all animations
- **Prohibited:** Core `Animated` API, native driver for complex sequences
- **Principles:**
  - Smooth 60fps animations
  - Platform-appropriate transitions
  - Consistent easing functions
  - No layout shifts during animations
- **Example:**
  ```typescript
  import Animated, { useSharedValue, useAnimatedStyle, withSpring } from 'react-native-reanimated';

  const offset = useSharedValue(0);

  const animatedStyle = useAnimatedStyle(() => ({
    transform: [{ translateY: offset.value }]
  }));

  const openBottomSheet = () => {
    offset.value = withSpring(300, {
      damping: 15,
      stiffness: 100
    });
  };
  ```

### 10. Route Transitions
- **Rule:** All screen transitions via `expo-router` layout animations
- **Implementation:**
  - Slide transitions for navigation
  - Scale transitions for modals
  - Fade transitions for overlays
- **Example:**
  ```typescript
  // app/station/[id].tsx
  import { useRouter } from 'expo-router';
  import { Animated } from 'react-native-reanimated';

  const router = useRouter();
  const slideAnim = useRef(new Animated.Value(-100)).current;

  useEffect(() => {
    Animated.spring(slideAnim, {
      toValue: 0,
      speed: 0.5,
      useNativeDriver: true
    }).start();
  }, []);

  const handlePress = () => {
    router.push(`/station/${stationId}`);
  };
  ```

---

## Design Token Discipline

### Rule 11: No Hardcoded Tokens
- **Rule:** All design tokens must be defined in `source/mobile-driver/design/tokens.ts`
- **Enforcement:** LLM must reference tokens file before inventing values
- **Examples:**
  ```typescript
  // ✗ WRONG - hardcoded values
  <View style={{ backgroundColor: '#3b82f6', padding: 16 }}>
    <Text style={{ fontSize: 16, color: '#ffffff' }}>
      Button
    </Text>
  </View>

  // ✗ WRONG - inline variant checks
  <View style={darkMode ? { backgroundColor: '#1a1a1a' } : { backgroundColor: '#ffffff' }}>
    <Text style={darkMode ? { color: '#ffffff' } : { color: '#000000' }}>
      Text
    </Text>
  </View>

  // ✓ CORRECT - uses tokens
  <View style={{ backgroundColor: tokens.colors.background }}>
    <Text style={{ color: tokens.colors.text }}>
      Button
    </Text>
  </View>
  ```

### Token Structure
```typescript
// tokens.ts
export const tokens = {
  colors: {
    light: {
      background: '#ffffff',
      surface: '#f3f4f6',
      primary: '#3b82f6',
      text: '#000000',
      // ... more colors
    },
    dark: {
      background: '#000000',
      surface: '#1a1a1a',
      primary: '#60a5fa',
      text: '#ffffff',
      // ... more colors
    }
  },
  spacing: {
    xs: 4,
    sm: 8,
    md: 16,
    lg: 24,
    xl: 32,
    // ... more spacing
  },
  typography: {
    // ... more typography tokens
  },
  border: {
    radius: 12,
    // ... more border tokens
  }
};
```

---

## Component Guidelines

### Skeleton Components
1. **SkeletonBox** — Single animated block
2. **SkeletonGroup** — Multiple aligned blocks
3. **Screen-specific skeletons:**
   - `StationListItemSkeleton`
   - `StationDetailSkeleton`
   - `SearchBarSkeleton`
   - `StationListSkeleton`
   - `MapLoadingSkeleton`

### Error Components
1. **EmptyState** — Empty result states
2. **ErrorState** — Failure states with recovery
3. **ConnectionError** — Network failures
4. **PermissionError** — Access denied

### Gesture Components
1. **BottomSheet** — Draggable sheet with threshold
2. **SwipeAction** — Swipe-to-reveal actions
3. **PullToRefresh** — List pull-to-refresh
4. **SwipeBack** — Gesture-based navigation

---

## Quality Standards

### Performance
- **Scroll:** 60fps with any content list
- **Animations:** 60fps, no jank
- **Map:** 1000+ markers with no jitter
- **Startup:** <2 seconds from splash to content

### Accessibility
- **Contrast:** WCAG AA compliant
- **Touch Targets:** Min 44x44 points
- **Labeling:** All interactive elements labeled
- **Navigation:** Screen reader compatible

### Consistency
- **Animations:** Same easing across all screens
- **Spacing:** Consistent rhythm throughout app
- **Colors:** Same meaning across components
- **Typography:** Consistent hierarchy and weights

---

## Testing Checklist

- [ ] Every screen has skeleton screens
- [ ] Every primary CTA has haptics
- [ ] Every error state has recovery action
- [ ] Every empty state is designed
- [ ] Dark mode works on all screens
- [ ] Map has no marker jitter
- [ ] All animations use reanimated v3
- [ ] No hardcoded colors/spacing
- [ ] No Platform.OS checks outside MapContainer
- [ ] All gestures smooth and responsive

---

## Implementation Rules

1. **First:** Read this skill and the constitution
2. **Second:** Check existing components in `source/mobile-driver/`
3. **Third:** Define new tokens in `tokens.ts` if needed
4. **Fourth:** Build component using tokens and reanimated
5. **Fifth:** Add haptics to primary actions
6. **Sixth:** Test in both light and dark modes
7. **Seventh:** Verify animations are 60fps
8. **Eighth:** Test on real device

---

## Related Files

- **Constitution:** `docs/constitution-v1.0.md` — Section 7
- **Design Tokens:** `source/mobile-driver/design/tokens.ts`
- **Theme Provider:** `source/mobile-driver/design/theme.ts`
- **Existing Components:** `source/mobile-driver/components/`
- **API Contract:** `docs/api-contract.md`

---

## Rules Enforcement

This skill is enforced by:
1. **Constitution:** Section 7 of constitution references this skill
2. **LLM Instructions:** AI agents must consult this skill before generating UI code
3. **Code Review:** All UI changes reviewed against these rules
4. **Testing:** Automated checks for spinner usage, hardcoded values, missing haptics

---

**Version:** 1.0  
**Last Updated:** 2026-06-11  
**Enforcer:** BorneMap Constitution Section 7
