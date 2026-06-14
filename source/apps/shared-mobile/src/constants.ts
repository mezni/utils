// source/apps/shared-mobile/src/constants.ts

/**
 * Strict bounding box enclosing Tunisia's core operational infrastructure.
 * Rejects arbitrary or erroneous coordinates at the UI perimeter.
 */
export const TUNISIA_GEO_BOUNDS = {
  MIN_LON: 7.0000,
  MAX_LON: 12.0000,
  MIN_LAT: 30.0000,
  MAX_LAT: 38.0000,
} as const;

/**
 * Initial viewport framing lens centered over Tunis for react-native-maps.
 */
export const TUNIS_INITIAL_REGION = {
  latitude: 36.8065,
  longitude: 10.1815,
  latitudeDelta: 0.05,
  longitudeDelta: 0.05,
} as const;

/**
 * Valid operational states for physical charging nodes.
 */
export const STATION_AVAILABILITY = {
  AVAILABLE: 'AVAILABLE',
  OCCUPIED: 'OCCUPIED',
  OUT_OF_SERVICE: 'OUT_OF_SERVICE',
} as const;

/**
 * Live execution statuses for child hardware plugs.
 */
export const CHARGER_STATUS = {
  ONLINE: 'ONLINE',
  CHARGING: 'CHARGING',
  FAULTED: 'FAULTED',
  OFFLINE: 'OFFLINE',
} as const;
