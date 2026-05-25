# BorneMap — Driver Mobile App UX & Geospatial Spec

## 1. Technical Stack Constraints

| Constraint | Requirement |
|------------|-------------|
| Workflow Framework | Managed workflow powered exclusively through **Expo Go** |
| Ejection Barricade | **Never** execute `expo eject` or `expo prebuild`. The project must remain within pure JS/TS operational boundaries. |
| Native Compilation | Native compilation switches or raw environment mutations are **explicitly banned**. |

### Dependency Lock Array

All dependencies use **invariant exact versions** to avoid native runtime drift:

```json
{
  "dependencies": {
    "expo": "~51.0.0",
    "expo-router": "~3.5.0",
    "react-native-maps": "1.14.0",
    "expo-location": "~17.0.0",
    "@gorhom/bottom-sheet": "~4.6.0",
    "react-native-reanimated": "~3.10.0",
    "react-native-gesture-handler": "~2.16.0",
    "expo-haptics": "~13.0.0",
    "@react-native-async-storage/async-storage": "1.23.1"
  }
}
```

## 2. Discovery Invariants & Constraints

### Nearby Search Baseline Geometry

The frontend request to the `/api/v1/stations/nearby` router applies a **default execution radius of 20km** (`radius=20000.0`) unless manually specified by user filtering configuration profiles.

### Pagination Hard Cap

The maximum record result count parsed and displayed back to the mapping engine is strictly bounded at **50 entries** (`LIMIT 50`) to optimize UI processing loops and device rendering speeds.

### Isolation Invariant

Production mobile instances **completely hide** any location record flagged as a test target (`is_test = true`). The `include_test` query parameter defaults to `false` on the backend, ensuring no test data leaks to the mobile app without explicit opt-in.
