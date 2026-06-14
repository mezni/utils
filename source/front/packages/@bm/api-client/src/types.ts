import type { Station } from '@bm/types'

export interface StationResponse {
  id: string
  name: string
  status: 'active' | 'maintenance'
  latitude: number
  longitude: number
  location: {
    type: 'Point'
    coordinates: [number, number]
  }
  distance?: number
}

export function toStation(raw: StationResponse): Station {
  return {
    id: raw.id,
    name: raw.name,
    status: raw.status,
    latitude: raw.latitude,
    longitude: raw.longitude,
    location: raw.location,
    distance: raw.distance,
  }
}

export function validateLat(lat: number): void {
  if (lat < -90 || lat > 90) {
    throw new RangeError(`latitude must be in [-90, 90], got ${lat}`)
  }
}

export function validateLng(lng: number): void {
  if (lng < -180 || lng > 180) {
    throw new RangeError(`longitude must be in [-180, 180], got ${lng}`)
  }
}

export function validateRadius(radius: number): void {
  if (radius <= 0) {
    throw new RangeError(`radius must be > 0, got ${radius}`)
  }
}

export function validateId(id: string): void {
  if (!id || typeof id !== 'string') {
    throw new RangeError('id must be a non-empty string')
  }
}
