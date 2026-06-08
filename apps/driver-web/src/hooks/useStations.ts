import { useState, useEffect } from 'react';
import type { Station, StationMapState } from '../types/station';
import { fetchStationsNearby } from '../services/api';

const DEFAULT_CENTER = { lat: 34.0, lng: 9.0 };

export function useStations(): StationMapState {
  const [state, setState] = useState<StationMapState>({
    stations: [],
    loading: true,
    error: null,
  });

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        setState((prev) => ({ ...prev, loading: true, error: null }));
        const stations: Station[] = await fetchStationsNearby(
          DEFAULT_CENTER.lat,
          DEFAULT_CENTER.lng,
          200
        );
        if (!cancelled) {
          setState({ stations, loading: false, error: null });
        }
      } catch (err) {
        if (!cancelled) {
          setState({
            stations: [],
            loading: false,
            error:
              err instanceof Error
                ? err.message
                : 'Unable to load stations',
          });
        }
      }
    }

    load();

    return () => {
      cancelled = true;
    };
  }, []);

  return state;
}
