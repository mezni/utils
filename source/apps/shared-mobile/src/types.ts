// source/apps/shared-mobile/src/types.ts

import { STATION_AVAILABILITY, CHARGER_STATUS } from './constants';

export type StationAvailabilityType = typeof STATION_AVAILABILITY[keyof typeof STATION_AVAILABILITY];
export type ChargerStatusType = typeof CHARGER_STATUS[keyof typeof CHARGER_STATUS];

export interface Coordinate {
  latitude: number;
  longitude: number;
}

/**
 * Sub-asset hardware plug contract matching the PostGIS aggregated rows.
 */
export interface ChargerDto {
  charger_id: string;
  code: string;
  plug_type: string;
  max_power_kw: number;
  status: ChargerStatusType;
}

/**
 * Primary geospatial station packet consumed by your hardware-accelerated maps.
 */
export interface NearbyStationDto {
  station_id: string;
  station_name: string;
  station_address: string | null;
  distance_meters: number;
  latitude: number;
  longitude: number;
  available_chargers: ChargerDto[];
}
