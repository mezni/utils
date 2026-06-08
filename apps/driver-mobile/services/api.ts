const API_BASE = 'http://localhost:3001/api/v1';

export interface Station {
  id: string;
  name: string;
  latitude: number;
  longitude: number;
  address: string;
  available_chargers: number;
  total_chargers: number;
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

  const data = await res.json();
  return data.stations as Station[];
}
