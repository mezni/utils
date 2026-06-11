import { MapRegion, Station, ClickstreamEvent } from '../types';

const DRIVER_BASE =
  process.env.EXPO_PUBLIC_DRIVER_API_URL ?? 'http://localhost:8080';
const CLICKSTREAM_BASE =
  process.env.EXPO_PUBLIC_CLICKSTREAM_URL ?? 'http://localhost:8082';

async function request<T>(
  base: string,
  path: string,
  options?: RequestInit,
): Promise<T> {
  const res = await fetch(`${base}${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });
  if (!res.ok) {
    throw new Error(`API error ${res.status}: ${res.statusText}`);
  }
  return res.json();
}

export async function fetchStationsNearby(
  region: MapRegion,
): Promise<{ stations: Station[] }> {
  const radius_m = Math.round(
    Math.max(region.latitudeDelta, region.longitudeDelta) * 111_320 * 0.5,
  );
  return request<{ stations: Station[] }>(
    DRIVER_BASE,
    `/api/v1/stations/nearby?lat=${region.latitude}&lng=${region.longitude}&radius_m=${radius_m}`,
  );
}

export async function fetchStationDetail(
  id: string,
): Promise<Station> {
  return request<Station>(DRIVER_BASE, `/api/v1/stations/${id}`);
}

export async function sendClickstreamEvent(
  event: ClickstreamEvent,
): Promise<void> {
  await request<void>(CLICKSTREAM_BASE, '/api/v1/events', {
    method: 'POST',
    body: JSON.stringify(event),
  });
}
