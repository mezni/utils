import { useState, useEffect } from "react";

interface Station {
  station_id: string;
  name: string;
  distance_meters: number;
}

interface UseNearbyStationsResult {
  stations: Station[];
  loading: boolean;
  error: string | null;
  retry: () => void;
}

export function useNearbyStations(
  lat: number,
  lng: number,
  radius: number
): UseNearbyStationsResult {
  const [stations, setStations] = useState<Station[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [retryCount, setRetryCount] = useState(0);

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setError(null);

    fetch(`/api/v1/driver/nearby?lat=${lat}&lng=${lng}&radius=${radius}`, {
      signal: controller.signal,
    })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then((data: Station[]) => {
        setStations(data);
        setLoading(false);
      })
      .catch((err) => {
        if (err.name !== "AbortError") {
          setError(err.message);
          setLoading(false);
        }
      });

    return () => controller.abort();
  }, [lat, lng, radius, retryCount]);

  const retry = () => setRetryCount((c) => c + 1);

  return { stations, loading, error, retry };
}
