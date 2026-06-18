import Constants from 'expo-constants'
import { Station, ValidationErrors } from '../types'
import { isWithinTunisia } from '../utils/coordinates'

const API_BASE_URL: string =
  (Constants.expoConfig as { extra?: { apiBaseUrl?: string } } | undefined)?.extra
    ?.apiBaseUrl ?? 'http://localhost:3001'

export function getBaseUrl(): string {
  return API_BASE_URL
}

export function validateCoordinates(
  lat: number,
  lng: number,
  radius: number,
): ValidationErrors {
  const errors: ValidationErrors = {}

  if (typeof lat !== 'number' || lat < -90 || lat > 90) {
    errors.latitude = 'Latitude must be between -90 and 90'
  }
  if (typeof lng !== 'number' || lng < -180 || lng > 180) {
    errors.longitude = 'Longitude must be between -180 and 180'
  }
  if (typeof radius !== 'number' || radius < 1 || radius > 200000) {
    errors.radius = 'Radius must be between 1 and 200000 meters'
  }

  if (!errors.latitude && !errors.longitude && !isWithinTunisia(lat, lng)) {
    errors.latitude = 'Coordinates are outside Tunisia bounds'
  }

  return errors
}

export async function fetchNearbyStations(
  lat: number,
  lng: number,
  radius: number,
  signal?: AbortSignal,
): Promise<Station[]> {
  const validationErrors = validateCoordinates(lat, lng, radius)
  if (Object.keys(validationErrors).length > 0) {
    throw new Error(
      Object.values(validationErrors).join('; '),
    )
  }

  const params = new URLSearchParams({
    lat: lat.toString(),
    lng: lng.toString(),
    radius: radius.toString(),
  })

  const response = await fetch(`${API_BASE_URL}/api/v1/nearby?${params}`, {
    method: 'GET',
    signal,
    headers: { Accept: 'application/json' },
  })

  if (!response.ok) {
    throw new Error(`API error: ${response.status} ${response.statusText}`)
  }

  const data: { stations: Station[] } = await response.json()
  return data.stations
}
