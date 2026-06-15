export const TUNISIA_GEO_BOUNDS = {
  MIN_LON: 7.0000,
  MAX_LON: 12.0000,
  MIN_LAT: 30.0000,
  MAX_LAT: 38.0000,
} as const;

export const TUNIS_INITIAL_REGION = {
  latitude: 36.8065,
  longitude: 10.1815,
  latitudeDelta: 0.05,
  longitudeDelta: 0.05,
} as const;

export const TUNIS_INITIAL_CENTER = {
  lat: 36.8065,
  lng: 10.1815,
  zoom: 10,
} as const;

export const STATION_AVAILABILITY = {
  AVAILABLE: 'AVAILABLE',
  OCCUPIED: 'OCCUPIED',
  OUT_OF_SERVICE: 'OUT_OF_SERVICE',
} as const;

export const CHARGER_STATUS = {
  ONLINE: 'ONLINE',
  CHARGING: 'CHARGING',
  FAULTED: 'FAULTED',
  OFFLINE: 'OFFLINE',
} as const;

export const API_BASE_URL = __DEV__
  ? 'http://localhost:3001'
  : 'http://bornemap.local/api/v1/driver';
