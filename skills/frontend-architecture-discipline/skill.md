# Frontend Architecture Discipline Skill — BorneMap

## Purpose
Prevent frontend chaos through strict separation, strict patterns, and strict architecture.

---

## 🎯 Core Philosophy

**Frontend is not a collection of screens. It is a reactive system driven by data and state.**

---

## 🔒 Core Rules

### 1. MapContainer is the ONLY Map Abstraction

**No direct map library usage anywhere else:**

```typescript
// ❌ WRONG - Direct map usage
// components/StationMarker.tsx
import MapView from 'react-native-maps';  // ❌ Direct usage
import Leaflet from 'leaflet';  // ❌ Direct usage

function StationMarker({ station }) {
  return (
    <MapView.Marker coordinate={{...}}>
      {/* ❌ Direct map logic */}
    </MapView.Marker>
  );
}

// ✅ CORRECT - Use MapContainer abstraction
import { MapContainer } from '@bm/map-container';

function StationMarker({ station }) {
  return (
    <MapContainer.Marker station={station}>
      {/* ✅ Business logic only */}
    </MapContainer.Marker>
  );
}
```

**MapContainer Responsibilities:**
- Platform-specific map library integration
- Marker rendering
- Map interactions (pan, zoom, tap)
- Platform abstractions
- Performance optimizations

**Forbiddens:**
- ❌ Direct map library import in UI components
- ❌ Platform-specific logic in UI components
- ❌ Map event handlers outside MapContainer
- ❌ Map coordinate calculations outside MapContainer

---

### 2. No Direct API Calls in Components

**All API calls go through @bm/api-client:**

```typescript
// ❌ WRONG - Direct API call
function StationList() {
  const [stations, setStations] = useState([]);

  useEffect(() => {
    fetch('/api/v1/stations')
      .then(res => res.json())
      .then(data => setStations(data));
  }, []);

  return (
    <ul>
      {stations.map(station => (
        <li key={station.id}>{station.name}</li>
      ))}
    </ul>
  );
}

// ✅ CORRECT - Use API client
import { getStations } from '@bm/api-client';
import { useStations } from '@bm/hooks';

function StationList() {
  const { data, loading, error } = useStations();

  if (loading) return <Skeleton />;

  return (
    <ul>
      {data?.stations.map(station => (
        <li key={station.id}>{station.name}</li>
      ))}
    </ul>
  );
}
```

**API Client Responsibilities:**
- API endpoint definitions
- Request/response handling
- Error handling
- Type validation
- Retry logic

**Forbiddens:**
- ❌ fetch() or axios in components
- ❌ Direct backend calls
- ❌ API logic duplication
- ❌ No error handling

---

### 3. Strict State Separation

**Three layers of state:**

```
UI State (Zustand)
   ↓
Server State (React Query)
   ↓
API Client
   ↓
Backend
```

**UI State (Zustand):**
- Local UI state
- Temporary state
- User interactions
- Selection state

**Server State (React Query):**
- Server data
- Caching
- Refreshing
- Loading states

**API Client:**
- API definitions
- Request handling
- Type definitions

---

### 4. No Platform Branching Outside Adapters

**Platform logic lives in adapters:**

```typescript
// ❌ WRONG - Platform logic in components
function StationMarker({ station }) {
  if (isMobile) {
    return (
      <MapView.Marker
        onPress={() => onSelect(station)}
      >
        {/* ❌ Mobile logic here */}
      </MapView.Marker>
    );
  } else {
    return (
      <Leaflet.Marker
        onClick={() => onSelect(station)}
      >
        {/* ❌ Web logic here */}
      </Leaflet.Marker>
    );
  }
}

// ✅ CORRECT - Platform adapters
import { MapContainer } from '@bm/map-container';
import { usePlatform } from '@bm/utils';

function StationMarker({ station }) {
  const { platform } = usePlatform();

  return (
    <MapContainer.Marker
      platform={platform}
      station={station}
      onPress={() => onSelect(station)}
    />
  );
}

// @bm/map-container/MapContainer.native.tsx
export function MapContainer({ platform, children }) {
  const MapView = platform === 'mobile' ? NativeMapView : WebMapView;
  return <MapView>{children}</MapView>;
}
```

**Platform Adapters:**
- Platform detection
- Platform-specific implementations
- Platform event handling
- Platform optimizations

**Forbiddens:**
- ❌ Platform checks in UI components
- ❌ Platform-specific logic in components
- ❌ Platform branches in hooks
- ❌ Inline platform detection

---

### 5. No UI Logic in Hooks That Mix Domains

**Hooks must be domain-specific:**

```typescript
// ❌ WRONG - Mixed domain logic
function useStationList() {
  // ❌ Both server state + business logic
  const { data, isLoading, error } = useStations();

  const filteredStations = useMemo(() => {
    if (isLoading) return [];
    // ❌ Business logic mixed with state management
    return data.stations.filter(s => s.status === 'active');
  }, [data, isLoading]);

  return { filteredStations, isLoading, error };
}

// ✅ CORRECT - Domain-specific hooks
// hooks/stations/useStationList.ts
export function useStationList() {
  // ✅ Only server state
  const { data, isLoading, error } = useStations();

  return { stations: data?.stations, isLoading, error };
}

// hooks/stations/useActiveStations.ts
export function useActiveStations() {
  const { stations, isLoading, error } = useStationList();

  // ✅ Pure business logic
  const activeStations = useMemo(() => {
    return stations?.filter(s => s.status === 'active') || [];
  }, [stations]);

  return { activeStations, isLoading, error };
}
```

**Hook Responsibilities:**
- Single domain concern
- Pure business logic
- No state management
- No UI logic

**Forbiddens:**
- ❌ Mixed server + business logic
- ❌ UI logic in hooks
- ❌ State management in hooks
- ❌ Domain logic duplication

---

### 6. No Duplicated UI Patterns Across Apps

**Single UI pattern library:**

```typescript
// ❌ WRONG - Duplicated patterns
// mobile-driver/src/components/StationMarker.tsx
function StationMarker({ station }) {
  return (
    <TouchableOpacity
      style={{
        backgroundColor: '#007AFF',  // ❌ Hardcoded color
        padding: 16,
        borderRadius: 8,  // ❌ Hardcoded spacing
      }}
      onPress={() => /*...*/}
    >
      <Text>{station.name}</Text>
    </TouchableOpacity>
  );
}

// web-driver/src/components/StationMarker.tsx
function StationMarker({ station }) {
  return (
    <div
      style={{
        backgroundColor: '#007AFF',  // ❌ Hardcoded color
        padding: '16px',  // ❌ Hardcoded spacing
        borderRadius: '8px',  // ❌ Hardcoded radius
      }}
      onClick={() => /*...*/}
    >
      <div>{station.name}</div>
    </div>
  );
}
```

**UI Pattern Library:**

```typescript
// ✅ CORRECT - Single pattern library
import { Button, Text } from '@bm/components';

function StationMarker({ station }) {
  return (
    <Button
      variant="station"
      onPress={() => onSelect(station)}
    >
      <Text variant="station-name">{station.name}</Text>
    </Button>
  );
}

// @bm/components/Button.tsx
export function Button({ variant, children, onPress }) {
  const styles = useStyles(variant);  // Uses design tokens

  return (
    <TouchableOpacity
      style={styles.container}
      onPress={onPress}
    >
      {children}
    </TouchableOpacity>
  );
}

// useStyles.ts
export function useStyles(variant) {
  switch (variant) {
    case 'station':
      return {
        container: {
          backgroundColor: colors.primary,
          padding: spacing.md,
          borderRadius: radius.md,
        },
        text: {
          fontSize: typography.body2.fontSize,
          color: colors.text,
        },
      };
    default:
      throw new Error(`Unknown variant: ${variant}`);
  }
}
```

**Forbiddens:**
- ❌ Duplicated components
- ❌ Duplicate styling logic
- ❌ Duplicate event handling
- ❌ Duplicate state management

---

### 7. No Inline Styling System

**All styling through design tokens:**

```typescript
// ❌ WRONG - Inline styling
function StationMarker({ station }) {
  return (
    <View
      style={{
        padding: 16,  // ❌ Hardcoded spacing
        backgroundColor: '#007AFF',  // ❌ Hardcoded color
        borderRadius: 8,  // ❌ Hardcoded radius
      }}
    >
      <Text style={{ fontSize: 14, color: '#FFFFFF' }}>
        {station.name}
      </Text>
    </View>
  );
}

// ✅ CORRECT - Design tokens
import { colors, spacing, radius, typography } from '@bm/design-tokens';

function StationMarker({ station }) {
  return (
    <View
      style={{
        padding: spacing.md,
        backgroundColor: colors.primary,
        borderRadius: radius.md,
      }}
    >
      <Text style={typography.body2}>
        {station.name}
      </Text>
    </View>
  );
}
```

**Forbiddens:**
- ❌ Inline colors (except layout glue)
- ❌ Inline spacing
- ❌ Inline typography
- ❌ Custom radius calculations

---

## 🚫 Anti-Patterns

### 1. Direct Map Library Usage
```typescript
// ❌ WRONG
import MapView from 'react-native-maps';
function Marker() {
  return <MapView.Marker coordinate={{...}} />;
}
```

### 2. Direct API Calls
```typescript
// ❌ WRONG
function Stations() {
  useEffect(() => {
    fetch('/api/v1/stations').then(res => res.json());
  }, []);
}
```

### 3. Platform Logic in Components
```typescript
// ❌ WRONG
function Marker() {
  const isMobile = Platform.OS === 'ios';
  if (isMobile) { /*...*/ }
}
```

### 4. Mixed State Management
```typescript
// ❌ WRONG
function useStations() {
  const [stations, setStations] = useState([]);  // ❌ UI state
  const { data } = useStationsAPI();  // ❌ Server state
  // ❌ Mixing
}
```

### 5. Duplicated Components
```typescript
// ❌ WRONG
// StationMarker in mobile and web - different implementations
```

---

## 🎯 Frontend Architecture Checklist

**Before implementing ANY component:**

- [ ] Uses MapContainer for map logic
- [ ] Uses @bm/api-client for API calls
- [ ] Separates UI state (Zustand) from server state (React Query)
- [ ] Platform logic in adapters
- [ ] No platform branching in components
- [ ] Domain-specific hooks
- [ ] No UI logic in hooks
- [ ] Uses design tokens for styling
- [ ] No inline styles (except layout glue)
- [ ] No duplicated components

---

## 🔄 Architecture Enforcement

**Before Writing Code:**

```
1. Identify Component Type
   - UI component?
   - Hook?
   - Hook is domain-specific?
   ↓
2. Check Map Usage
   - Uses MapContainer? ✅ or ❌
   - Direct map usage? ❌ STOP
   ↓
3. Check API Usage
   - Uses @bm/api-client? ✅ or ❌
   - Direct API calls? ❌ STOP
   ↓
4. Check State Management
   - UI state: Zustand? ✅
   - Server state: React Query? ✅
   - Mixed? ❌ STOP
   ↓
5. Check Platform Logic
   - Platform logic in adapters? ✅
   - Platform branching in components? ❌ STOP
   ↓
6. Check Hook Logic
   - Domain-specific? ✅
   - UI logic included? ❌ STOP
   ↓
7. Check Styling
   - Uses design tokens? ✅
   - No inline styles? ✅
```

---

*This skill prevents frontend chaos through strict architecture and patterns.*