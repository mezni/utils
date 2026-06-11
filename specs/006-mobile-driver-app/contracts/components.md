# Component Contracts: Mobile Driver App (Core UX)

## Hook Contracts

```tsx
// useNearbyStations — fetches stations for a map region
import { useNearbyStations } from '../src/hooks/useNearbyStations';

function MapScreen() {
  const { stations, loading, error, refetch } = useNearbyStations();

  useEffect(() => {
    // Initial fetch on mount
    refetch(initialRegion);
  }, []);

  const onRegionChangeComplete = (region: MapRegion) => {
    refetch(region);
  };

  if (loading) return <Skeleton variant="map" />;
  if (error) return <ErrorState message={error} onRetry={() => refetch(currentRegion)} />;
  if (stations.length === 0) return <EmptyState title="No stations nearby" />;

  return <MapView markers={stations} />;
}
```

```tsx
// useStationDetail — fetches single station with chargers
import { useStationDetail } from '../src/hooks/useStationDetail';

function StationBottomSheet({ stationId }: { stationId: string }) {
  const { station, loading, error, refetch } = useStationDetail();

  useEffect(() => {
    refetch(stationId);
  }, [stationId]);

  if (loading) return <Skeleton variant="list" rows={3} />;
  if (error) return <ErrorState message={error} onRetry={() => refetch(stationId)} />;

  return <StationContent station={station} />;
}
```

```tsx
// useLocation — GPS permission + current location
import { useLocation } from '../src/hooks/useLocation';

function MapScreen() {
  const { location, permissionDenied, error } = useLocation();

  if (permissionDenied) {
    return <EmptyState title="GPS unavailable" description="Enable location services to find nearby stations" />;
  }

  return <MapView initialRegion={regionFromLocation(location)} />;
}
```

```tsx
// useClickstream — fire-and-forget event tracking
import { useClickstream } from '../src/hooks/useClickstream';

function MapScreen() {
  const { track } = useClickstream();

  useEffect(() => {
    track({ event_type: 'map_open', timestamp: new Date().toISOString() });
  }, []);

  const onMarkerPress = (station: Station) => {
    track({ event_type: 'station_click', timestamp: new Date().toISOString(), station_id: station.id });
  };
}
```

## API Service Contracts

```ts
// services/api.ts — Driver Service client
import { api } from '../src/services/api';

// GET /api/v1/stations/nearby?lat=36.8&lng=10.18&radius_m=5000
const response = await api.get('/api/v1/stations/nearby', {
  params: { lat: 36.8, lng: 10.18, radius_m: 5000 },
});
// Response: { stations: Station[] }

// GET /api/v1/stations/{id}
const response = await api.get(`/api/v1/stations/${stationId}`);
// Response: Station (with chargers array)
```

```ts
// services/api.ts — Clickstream Service client
import { api } from '../src/services/api';

// POST /api/v1/events (fire-and-forget)
api.post('/api/v1/events', {
  event_type: 'map_open',
  timestamp: new Date().toISOString(),
}).catch(() => {}); // Silent catch — never blocks UX
```
