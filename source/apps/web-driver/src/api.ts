interface ChargerDto {
  charger_id: string;
  code: string;
  plug_type: string;
  max_power_kw: number;
  status: string;
}

interface NearbyStationDto {
  station_id: string;
  station_name: string;
  station_address: string | null;
  distance_meters: number;
  latitude: number;
  longitude: number;
  available_chargers: ChargerDto[];
}

interface NearbyStationsResponse {
  stations: NearbyStationDto[];
}

const API_BASE = '/api/v1';

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

export type { NearbyStationDto, ChargerDto };
