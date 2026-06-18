export interface Station {
  station_id: string
  station_name: string
  latitude: number
  longitude: number
  distance_meters: number
  partner_name: string
  is_private: boolean
}

export interface Viewport {
  latitude: number
  longitude: number
  latitudeDelta: number
  longitudeDelta: number
  zoomLevel: number
  lastUpdated: number
}

export interface AsyncCacheEntry {
  viewportKey: string
  stations: Station[]
  cachedAt: number
  viewportCenter: { lat: number; lng: number }
}

export interface ValidationErrors {
  latitude?: string
  longitude?: string
  radius?: string
}

export type ApiFetchState =
  | { type: 'loading' }
  | { type: 'success'; stations: Station[] }
  | { type: 'empty' }
  | { type: 'error'; message: string }
  | { type: 'offline'; stations: Station[] }
