import { useState, useCallback } from 'react';
import { MapRegion, Station, UseNearbyStationsResult } from '../types';
import { fetchStationsNearby } from '../services/api';

export function useNearbyStations(): UseNearbyStationsResult {
  const [stations, setStations] = useState<Station[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const refetch = useCallback(async (region: MapRegion) => {
    setLoading(true);
    setError(null);
    try {
      const data = await fetchStationsNearby(region);
      setStations(data.stations);
    } catch (e: unknown) {
      setError(
        e instanceof Error ? e.message : 'Failed to load stations',
      );
    } finally {
      setLoading(false);
    }
  }, []);

  return { stations, loading, error, refetch };
}
