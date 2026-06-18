export const TUNISIA_BOUNDS = {
  minLat: 30,
  maxLat: 38,
  minLng: 7,
  maxLng: 12,
} as const

export const DEFAULT_VIEWPORT = {
  latitude: 36.8,
  longitude: 10.18,
  latitudeDelta: 0.1,
  longitudeDelta: 0.1,
  zoomLevel: 12,
  lastUpdated: Date.now(),
} as const

export function isWithinTunisia(lat: number, lng: number): boolean {
  return (
    lat >= TUNISIA_BOUNDS.minLat &&
    lat <= TUNISIA_BOUNDS.maxLat &&
    lng >= TUNISIA_BOUNDS.minLng &&
    lng <= TUNISIA_BOUNDS.maxLng
  )
}

export function roundTo2dp(value: number): number {
  return Math.round(value * 100) / 100
}

export function clampToTunisia(lat: number, lng: number): { lat: number; lng: number } {
  return {
    lat: Math.min(Math.max(lat, TUNISIA_BOUNDS.minLat), TUNISIA_BOUNDS.maxLat),
    lng: Math.min(Math.max(lng, TUNISIA_BOUNDS.minLng), TUNISIA_BOUNDS.maxLng),
  }
}
