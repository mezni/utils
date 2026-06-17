import { useState, useEffect, useCallback, useRef } from 'react';
import type { NearbyResponse, ErrorResponse, Location } from '@bornemap/shared-types';
import { getNearby } from '@bornemap/api-client';

interface UseNearbyOptions {
  enabled?: boolean;
  radius_m?: number;
  max_results?: number;
  visibility?: 'commercial' | 'private_home' | 'all';
  debounceMs?: number;
}

interface UseNearbyResult {
  stations: (NearbyResponse['stations'][0]) | null;
  error: ErrorResponse | null;
  loading: boolean;
  refetch: (location: Location) => void;
}

const DEBOUNCE_MS = 300;

export function useNearby(
  location: Location | null,
  options: UseNearbyOptions = {}
): UseNearbyResult {
  const {
    enabled = true,
    radius_m = 5000,
    max_results = 50,
    visibility = 'all',
    debounceMs = DEBOUNCE_MS,
  } = options;

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<ErrorResponse | null>(null);
  const [stations, setStations] = useState<(NearbyResponse['stations'][0]) | null>(null);

  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const mountedRef = useRef(true);

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current);
      }
    };
  }, []);

  const fetchNearby = useCallback(async (loc: Location) => {
    if (!loc || !enabled) return;

    setLoading(true);
    setError(null);

    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
    }

    timeoutRef.current = setTimeout(async () => {
      if (!mountedRef.current) return;

      try {
        const data = await getNearby({
          lat: loc.lat,
          lon: loc.lon,
          radius_m,
          max_results,
          visibility,
        });

        if (mountedRef.current) {
          if ('error' in data) {
            setError(data);
          } else {
            setStations(data.stations[0] || null);
          }
          setLoading(false);
        }
      } catch (err) {
        if (mountedRef.current) {
          setError({
            error: {
              code: 'NETWORK_ERROR',
              message: 'Failed to connect to the server',
            },
            meta: {
              request_id: 'unknown',
              timestamp: new Date().toISOString(),
            },
          });
          setLoading(false);
        }
      }
    }, debounceMs);
  }, [enabled, radius_m, max_results, visibility, debounceMs]);

  useEffect(() => {
    if (location) {
      fetchNearby(location);
    }
  }, [location, fetchNearby]);

  return { stations, error, loading, refetch: fetchNearby };
}

interface UseNearbyStationsResult {
  stations: NearbyResponse['stations'];
  error: ErrorResponse | null;
  loading: boolean;
  count: number;
  refetch: (location: Location) => void;
}

export function useNearbyStations(
  location: Location | null,
  options: UseNearbyOptions = {}
): UseNearbyStationsResult {
  const {
    enabled = true,
    radius_m = 5000,
    max_results = 50,
    visibility = 'all',
    debounceMs = DEBOUNCE_MS,
  } = options;

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<ErrorResponse | null>(null);
  const [stations, setStations] = useState<NearbyResponse['stations']>([]);
  const [count, setCount] = useState(0);

  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const mountedRef = useRef(true);

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current);
      }
    };
  }, []);

  const fetchNearby = useCallback(async (loc: Location) => {
    if (!loc || !enabled) return;

    setLoading(true);
    setError(null);

    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
    }

    timeoutRef.current = setTimeout(async () => {
      if (!mountedRef.current) return;

      try {
        const data = await getNearby({
          lat: loc.lat,
          lon: loc.lon,
          radius_m,
          max_results,
          visibility,
        });

        if (mountedRef.current) {
          if ('error' in data) {
            setError(data);
          } else {
            setStations(data.stations);
            setCount(data.count);
          }
          setLoading(false);
        }
      } catch (err) {
        if (mountedRef.current) {
          setError({
            error: {
              code: 'NETWORK_ERROR',
              message: 'Failed to connect to the server',
            },
            meta: {
              request_id: 'unknown',
              timestamp: new Date().toISOString(),
            },
          });
          setLoading(false);
        }
      }
    }, debounceMs);
  }, [enabled, radius_m, max_results, visibility, debounceMs]);

  useEffect(() => {
    if (location) {
      fetchNearby(location);
    }
  }, [location, fetchNearby]);

  return { stations, error, loading, count, refetch: fetchNearby };
}
