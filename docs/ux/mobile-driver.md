# Driver Mobile UX

**Platform:** Expo SDK 54, react-native-maps, Reanimated v3

---

## Core Principles

- Map-first: the map is the primary UI surface
- Gesture-driven: navigation follows touch patterns, not buttons
- Skeletons over spinners: every load state shows skeleton placeholders
- Optimistic UI: mutations reflect immediately, sync in background
- Haptics: primary actions trigger haptic feedback
- Reanimated-only: all animations use react-native-reanimated v3

---

## Screen Map

```
App
├── MapScreen (default)      — map + nearby list
│   ├── MapContainer         — map platform adapter
│   ├── StationMarkers       — clustered markers
│   └── BottomSheet          — nearby station list
├── StationDetailScreen      — station info + chargers
├── FavoritesScreen (MVP-3)  — saved stations
└── ProfileScreen (MVP-3)    — user profile
```

---

## Map Interaction Model

| Gesture | Action |
|---|---|
| Pan | move map viewport |
| Pinch / zoom | zoom in/out |
| Tap marker | open station callout |
| Tap callout | navigate to StationDetail |
| Pull up | open nearby list (bottom sheet) |
| Long press (future) | drop pin / search here |

---

## Animation Rules

- All animations via Reanimated v3 shared value + animated style
- No Animated API from React Native
- No external animation libraries
- Transitions: 200-300ms ease-in-out
- Map gestures: native driver only

---

## Loading States

| Component | Loading State |
|---|---|
| Map | skeleton tile placeholder |
| Station list | 3 skeleton card rows |
| Station detail | skeleton detail card |
| Charger list | 2 skeleton charger rows |

---

## Design Token Rule

No hardcoded design tokens in components. All values from centralized theme system (future design system doc).
