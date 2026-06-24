import { fetchNearbyStations } from "@bornemap/client-core";
import type { StationDto } from "@bornemap/domain-types";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:3001";

export async function getNearbyStations(
  lat: number,
  lon: number,
  radius?: number,
  limit?: number,
): Promise<StationDto[]> {
  return fetchNearbyStations({
    baseUrl: API_BASE_URL,
    lat,
    lon,
    radius,
    limit,
  });
}
