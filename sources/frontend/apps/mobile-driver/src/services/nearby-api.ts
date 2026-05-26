import { Station, Charger } from "../types/station"
import Constants from "expo-constants"

const API_BASE = Constants.expoConfig?.extra?.apiUrl || "http://localhost:8080/api/v1"

export async function fetchNearbyStations(
  latitude: number,
  longitude: number,
  radiusMeters: number = 20000
): Promise<Station[]> {
  const url = `${API_BASE}/stations/nearby?latitude=${latitude}&longitude=${longitude}&radius_meters=${radiusMeters}`
  const res = await fetch(url)
  if (!res.ok) throw new Error("Failed to fetch nearby stations")
  const json = await res.json()
  const data = json.data || json
  return Array.isArray(data) ? data : []
}

export async function fetchStationChargers(stationId: string): Promise<Charger[]> {
  const url = `${API_BASE}/stations/${stationId}/chargers`
  const res = await fetch(url)
  if (!res.ok) throw new Error("Failed to fetch station chargers")
  const json = await res.json()
  const data = json.data || json
  return Array.isArray(data) ? data : []
}
