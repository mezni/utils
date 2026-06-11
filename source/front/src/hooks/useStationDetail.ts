import { useState, useCallback } from 'react';
import { Station, UseStationDetailResult } from '../types';
import { fetchStationDetail } from '../services/api';

export function useStationDetail(): UseStationDetailResult {
  const [station, setStation] = useState<Station | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const refetch = useCallback(async (id: string) => {
    setLoading(true);
    setError(null);
    try {
      const data = await fetchStationDetail(id);
      setStation(data);
    } catch (e: unknown) {
      setError(
        e instanceof Error ? e.message : 'Failed to load station details',
      );
    } finally {
      setLoading(false);
    }
  }, []);

  return { station, loading, error, refetch };
}
