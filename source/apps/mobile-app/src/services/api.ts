import { NearbyStationsResponse } from '../../../shared-mobile/src';

// Change to your machine's LAN IP when testing on a physical device
const DEVICE_HOST = '10.0.2.2'; // Android emulator; use e.g. '192.168.2.54' for physical

const API_BASE = `http://${DEVICE_HOST}:3001/api/v1`;

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
