import { NearbyStationsResponse } from '@bornemap/shared-types';

const API_BASE = 'http://localhost:3001/api/v1';

export async function fetchNearbyStations(
  latitude: number,
  longitude: number,
  radius: number = 5000
): Promise<NearbyStationsResponse> {
  const params = new URLSearchParams({
    latitude: latitude.toString(),
    longitude: longitude.toString(),
    radius: radius.toString(),
  });

  const response = await fetch(`${API_BASE}/stations/nearby?${params}`);

  if (!response.ok) {
    throw new Error(`API error: ${response.status}`);
  }

  return response.json();
}
