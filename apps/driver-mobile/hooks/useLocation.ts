import { useState, useEffect } from 'react';
import * as Location from 'expo-location';

const DEFAULT_COORDS = { latitude: 36.8065, longitude: 10.1815 };

interface LocationState {
  status: 'granted' | 'denied' | 'undetermined';
  coordinates: { latitude: number; longitude: number };
}

export function useLocation(): LocationState {
  const [state, setState] = useState<LocationState>({
    status: 'undetermined',
    coordinates: DEFAULT_COORDS,
  });

  useEffect(() => {
    let cancelled = false;

    async function requestLocation() {
      const { status } = await Location.requestForegroundPermissionsAsync();

      if (cancelled) return;

      if (status !== 'granted') {
        setState({ status: 'denied', coordinates: DEFAULT_COORDS });
        return;
      }

      const pos = await Location.getCurrentPositionAsync({});
      if (!cancelled) {
        setState({
          status: 'granted',
          coordinates: {
            latitude: pos.coords.latitude,
            longitude: pos.coords.longitude,
          },
        });
      }
    }

    requestLocation();

    return () => {
      cancelled = true;
    };
  }, []);

  return state;
}
