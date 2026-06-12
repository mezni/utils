# ADR-010: MapContainer Platform Abstraction

**Date:** 2026-06-11
**Status:** Accepted
**Decision:** Single `MapContainer.tsx` component abstracts react-native-maps (mobile) and leaflet (web).

---

## Context

The mobile driver app and web driver app need map functionality. We could:
1. Maintain two separate map components (one for mobile, one for web)
2. Use Platform.OS checks throughout the app
3. Create a single abstraction component

Option 3 prevents code duplication and ensures consistent map behavior across platforms.

---

## Decision

**Use a single `MapContainer.tsx` abstraction that handles both react-native-maps and leaflet.**

### Implementation

```typescript
// MapContainer.tsx
import { Platform } from 'react-native';
import ReactMap from 'react-native-maps';
import LeafletMap from 'leaflet';

export type MapProvider = 'native' | 'web';

interface MapContainerProps {
  children: React.ReactNode;
  // shared props for both platforms
  width?: number | string;
  height?: number | string;
}

export const MapContainer: React.FC<MapContainerProps> = ({
  children,
  width = '100%',
  height = '100%'
}) => {
  // Platform detection
  const isNative = Platform.OS === 'ios' || Platform.OS === 'android';
  const mapProvider: MapProvider = isNative ? 'native' : 'web';

  // Platform-specific rendering
  if (mapProvider === 'native') {
    return (
      <ReactMap
        style={{ width, height }}
        initialRegion={{
          latitude: 36.8065,
          longitude: 10.1815,
          latitudeDelta: 0.0922,
          longitudeDelta: 0.0421
        }}
      >
        {children}
      </ReactMap>
    );
  }

  // Web implementation using Leaflet
  return (
    <div style={{ width, height, position: 'relative' }}>
      <LeafletMap
        style={{ width: '100%', height: '100%' }}
        center={[36.8065, 10.1815]}
        zoom={13}
      >
        {children}
      </LeafletMap>
    </div>
  );
};
```

### Usage

```typescript
// ✗ WRONG - Platform.OS outside MapContainer
import { Platform } from 'react-native';

const Map = ({ lat, lng }) => {
  if (Platform.OS === 'ios') {
    return <MapView ... />;
  } else {
    return <Leaflet ... />;
  }
};

// ✓ CORRECT - uses abstraction
import { MapContainer } from '../components/MapContainer';

const MapScreen = ({ lat, lng }) => (
  <MapContainer>
    {/* Markers and overlay */}
  </MapContainer>
);
```

---

## Rules

1. **No `Platform.OS` checks outside `MapContainer.tsx`**
2. **All map components use `MapContainer`**
3. **Shared props and behavior across platforms**
4. **Platform-specific logic inside abstraction**

---

## Consequences

### Positive
- Single source of truth for map behavior
- No code duplication
- Consistent UX across platforms
- Easier maintenance and updates

### Negative
- One abstraction file to maintain
- Platform-specific bugs affect both platforms

---

## Alternatives Considered

### Alternative 1: Separate Components
```typescript
// mobile/Map.tsx
// web/Map.tsx
```

**Rejected:** Code duplication, inconsistent behavior, maintenance burden.

### Alternative 2: Platform.OS Throughout
```typescript
const [isNative, setIsNative] = useState(Platform.OS === 'ios');
```

**Rejected:** Scattered platform checks, breaking rule 11 (no hardcoded values).

---

## Implementation

1. Create `MapContainer.tsx` with platform detection
2. Replace all map usage with `MapContainer`
3. Remove Platform checks outside MapContainer
4. Test both mobile and web implementations

---

## Testing Checklist

- [ ] Mobile app renders map correctly
- [ ] Web app renders map correctly
- [ ] Markers display on both platforms
- [ ] Map interactions work identically
- [ ] No Platform.OS checks outside MapContainer
- [ ] Platform detection works across devices

---

## References

- **Constitution:** Section 6.4 — Map Strategy
- **Section 7.11:** No Platform.OS checks outside MapContainer
- **Mobile Stack:** Section 6.4
