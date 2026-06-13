# UX Principles

## Version: 1.0
## Status: Active
## Core Philosophy: Documentation is the system. Code is just its execution.

---

## 🎯 CORE PRINCIPLES

**UX is behavior, not visuals.**

**Design is systematic behavior under interaction.**

---

## 🗺️ MAP IS THE PRIMARY UI

### Core Principle

**The map is not a component. It is the interface.**

### Design Implications

1. **Map controls all flows**
   - Discovery starts on map
   - Selection happens on map
   - Navigation happens on map

2. **Map is always visible**
   - Never hide map behind overlays
   - Map controls all navigation
   - Map provides context always

3. **Map is interactive**
   - Always respond to gestures
   - Always provide feedback
   - Always maintain state

---

## 📍 EVERY ACTION IS GEOSPATIAL

### Core Principle

**User actions are location-based.**

### Design Implications

1. **Actions have geographic context**
   - Station selection is spatial
   - Nearby search is location-based
   - Station details are position-aware

2. **All results are spatial**
   - Stations displayed on map
   - Distances calculated accurately
   - Locations preserved always

3. **Context is always explicit**
   - User location shown
   - Search radius shown
   - Distance to stations shown

---

## 🚫 NO DEAD ENDS IN UI FLOWS

### Core Principle

**Users always have a clear path forward.**

### Design Rules

1. **Every screen has an exit**
   - Detail view has close button
   - No modal without dismiss
   - No stack without navigation

2. **Every action has feedback**
   - Taps are responsive
   - Changes are visible
   - Errors are clear

3. **Every state has recovery**
   - Loading states have retry
   - Error states have solutions
   - Empty states have options

---

## 💫 EVERY INTERACTION MUST HAVE FEEDBACK

### Core Principle

**Users always know what happened.**

### Feedback Types

#### 1. **Immediate Feedback**
- **Taps:** Haptic on mobile, visual click on web
- **Selection:** Highlight, shadow, animation
- **Loading:** Skeleton, spinner (no spinners for MVP-1)

#### 2. **Progress Feedback**
- **Data Fetch:** Loading state, progress bar
- **Animations:** Smooth transitions, no jank
- **Network:** Indicators for background operations

#### 3. **Outcome Feedback**
- **Success:** Green checkmark, toast message
- **Error:** Red border, clear message, retry
- **Empty:** Friendly message, helpful suggestion

---

## 🎨 UX RULES

### 1. Skeleton over Spinner

**❌ WRONG - Spinner:**

```typescript
// Don't do this
<ActivityIndicator size="large" />
<Text>Loading...</Text>
```

---

**✅ CORRECT - Skeleton:**

```typescript
// Do this
<View>
  <Skeleton variant="rectangular" height={200} />
  <Skeleton variant="text" width="80%" />
</View>
```

**Why:**
- Shows content structure
- Better for performance
- Professional appearance

---

### 2. Optimistic Updates Preferred

**Core Principle:**

**Show results before server confirms.**

### Example: Station Selection

**❌ WRONG - Blocking:**

```typescript
// Don't do this
const selectStation = async (station: Station) => {
  await api.getStationById(station.id);
  setSelectedStation(station);
  navigation.navigate('Detail');
};
```

---

**✅ CORRECT - Optimistic:**

```typescript
// Do this
const selectStation = (station: Station) => {
  // Optimistic update
  setSelectedStation(station);
  navigation.navigate('Detail');

  // Background fetch
  api.getStationById(station.id).catch(() => {
    // Handle error if fetch fails
    setSelectedStation(null);
  });
};
```

**Benefits:**
- Instant UI response
- Better perceived performance
- Maintains flow continuity

---

### 3. Haptics Required on Mobile

**Core Principle:**

**Every tap must have tactile feedback.**

### Haptic Triggers

1. **Marker tap**
   - `ImpactFeedbackLight` on press
   - `ImpactFeedbackMedium` on selection

2. **Button tap**
   - `ImpactFeedbackLight`

3. **Swipe dismiss**
   - `ImpactFeedbackMedium`

4. **Success action**
   - `NotificationFeedbackSuccess`

5. **Error action**
   - `NotificationFeedbackError`

---

### 4. Transitions Must Be Continuous

**Core Principle:**

**Animations should feel natural and smooth.**

### Animation Guidelines

- **Duration:** 150-300ms
- **Timing:** Ease-in-out
- **Frames:** 60fps minimum
- **Physics:** Natural motion

### Forbidden Animations

- No abrupt transitions
- No rapid zooming
- No janky animations
- No blocking animations

---

## 🎯 UX QUALITY METRICS

### Performance UX

- [ ] Tap response < 100ms
- [ ] Map panning smooth (60fps)
- [ ] Transitions < 300ms
- [ ] No UI freezing

### Feedback UX

- [ ] Every tap has feedback
- [ ] Every change is visible
- [ ] Every error is clear
- [ ] Every state has recovery

### Flow UX

- [ ] No dead ends
- [ ] Clear navigation paths
- [ ] Consistent patterns
- [ ] Predictable behavior

---

## 🧠 CORE PRINCIPLE

**UX is behavior, not visuals. Every interaction is designed.**

---

*This document ensures UX is systematic behavior, not just appearance.*