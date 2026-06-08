export interface Station {
  id: string;
  name: string;
  latitude: number;
  longitude: number;
  address: string;
  available_chargers: number;
  total_chargers: number;
}

export interface StationMapState {
  stations: Station[];
  loading: boolean;
  error: string | null;
}

export type Coordinates = {
  latitude: number;
  longitude: number;
};
