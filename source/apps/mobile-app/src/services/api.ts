import { NearbyStationsResponse } from '../../../shared-mobile/src';

import { Platform } from 'react-native';

const API_BASE = Platform.select({
  android: 'http://10.0.2.2:3001/api/v1',
  default: 'http://localhost:3001/api/v1',
});

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
