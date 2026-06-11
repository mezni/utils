export interface Station {
  id: string;
  name: string;
  latitude: number;
  longitude: number;
  distance_m: number | null;
  chargers: Charger[];
  address: string | null;
}

export interface Charger {
  id: string;
  connector_type: 'type2' | 'ccs' | 'chademo' | 'wall';
  power_kw: number;
  status: 'available' | 'occupied' | 'offline';
}

export interface MapRegion {
  latitude: number;
  longitude: number;
  latitudeDelta: number;
  longitudeDelta: number;
}

export interface ClickstreamEvent {
  event_type:
    | 'map_open'
    | 'map_pan'
    | 'map_zoom'
    | 'station_click'
    | 'station_view'
    | 'nearby_search';
  timestamp: string;
  station_id?: string | null;
  latitude?: number | null;
  longitude?: number | null;
  radius_m?: number | null;
}

export interface UseNearbyStationsResult {
  stations: Station[];
  loading: boolean;
  error: string | null;
  refetch: (region: MapRegion) => void;
}

export interface UseStationDetailResult {
  station: Station | null;
  loading: boolean;
  error: string | null;
  refetch: (id: string) => void;
}

export interface UseLocationResult {
  location: { latitude: number; longitude: number } | null;
  permissionDenied: boolean;
  error: string | null;
}

export interface UseClickstreamResult {
  track: (event: ClickstreamEvent) => void;
}
