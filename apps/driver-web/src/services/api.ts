import type { Station } from '../types/station';

const API_BASE = '/api/v1';

interface NearbyResponse {
  stations: Station[];
}

export async function fetchStationsNearby(
  lat: number,
  lng: number,
  radiusKm = 50
): Promise<Station[]> {
  const params = new URLSearchParams({
    lat: String(lat),
    lng: String(lng),
    radius_km: String(radiusKm),
  });

  const res = await fetch(`${API_BASE}/stations/nearby?${params}`);

  if (!res.ok) {
    throw new Error(`Failed to fetch stations: ${res.status}`);
  }

  const data: NearbyResponse = await res.json();
  return data.stations;
}
