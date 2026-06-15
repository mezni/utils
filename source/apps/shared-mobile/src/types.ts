import { STATION_AVAILABILITY, CHARGER_STATUS } from './constants';

export type StationAvailabilityType = typeof STATION_AVAILABILITY[keyof typeof STATION_AVAILABILITY];
export type ChargerStatusType = typeof CHARGER_STATUS[keyof typeof CHARGER_STATUS];

export interface Coordinate {
  latitude: number;
  longitude: number;
}

export interface ChargerDto {
  charger_id: string;
  code: string;
  plug_type: string;
  max_power_kw: number;
  status: ChargerStatusType;
}

export interface NearbyStationDto {
  station_id: string;
  station_name: string;
  station_address: string | null;
  distance_meters: number;
  latitude: number;
  longitude: number;
  available_chargers: ChargerDto[];
}

export interface NearbyStationsResponse {
  stations: NearbyStationDto[];
}
