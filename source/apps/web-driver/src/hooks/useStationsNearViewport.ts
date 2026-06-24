import { useState, useCallback, useRef, useEffect } from "react";
import { getNearbyStations } from "../services/stationService";
import type { StationDto } from "@bornemap/domain-types";

const TUNISIA_CENTER: [number, number] = [34.0, 9.5];
const DEFAULT_ZOOM = 6;
const DEFAULT_RADIUS = 50000;
const DEFAULT_LIMIT = 50;

export function useStationsNearViewport(debounceMs = 300) {
  const [center, setCenter] = useState<[number, number]>(TUNISIA_CENTER);
  const [zoom, setZoom] = useState(DEFAULT_ZOOM);
  const [stations, setStations] = useState<StationDto[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const mountedRef = useRef(true);

  useEffect(() => {
    return () => {
      mountedRef.current = false;
    };
  }, []);

  const fetchStations = useCallback(async (lat: number, lon: number) => {
    setIsLoading(true);
    setError(null);
    try {
      const data = await getNearbyStations(lat, lon, DEFAULT_RADIUS, DEFAULT_LIMIT);
      if (mountedRef.current) {
        setStations(data);
      }
    } catch (err) {
      if (mountedRef.current) {
        setError(err instanceof Error ? err : new Error("Failed to fetch stations"));
      }
    } finally {
      if (mountedRef.current) {
        setIsLoading(false);
      }
    }
  }, []);

  const onViewportChange = useCallback(
    (newCenter: [number, number], newZoom: number) => {
      setCenter(newCenter);
      setZoom(newZoom);

      if (timerRef.current) {
        clearTimeout(timerRef.current);
      }

      timerRef.current = setTimeout(() => {
        fetchStations(newCenter[0], newCenter[1]);
      }, debounceMs);
    },
    [fetchStations, debounceMs],
  );

  const refetch = useCallback(() => {
    fetchStations(center[0], center[1]);
  }, [fetchStations, center]);

  useEffect(() => {
    fetchStations(TUNISIA_CENTER[0], TUNISIA_CENTER[1]);
  }, [fetchStations]);

  return {
    center,
    zoom,
    stations,
    isLoading,
    error,
    onViewportChange,
    refetch,
  };
}
