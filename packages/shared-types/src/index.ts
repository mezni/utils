export type StationStatus = "draft" | "active" | "inactive" | "closed";
export type ChargerType = "ac" | "dc";
export type ConnectorStandard = "ccs2" | "type2" | "chademo";
export type ChargerStatus = 'available' | 'occupied' | 'offline' | 'maintenance';
export type Visibility = 'commercial' | 'private_home' | 'all';

export interface Location {
  lat: number;
  lon: number;
}

export interface Charger {
  id: string;
  connector_type: string;
  connector_count?: number;
  power_kw: number;
  status: ChargerStatus;
}

export interface Station {
  id: string;
  name: string;
  location: Location;
  address?: string;
  city: string;
  distance_m: number;
  visibility: Visibility;
  status: StationStatus;
  chargers?: Charger[];
}

export interface NearbyResponse {
  stations: Station[];
  count: number;
  radius_m: number;
}

export interface ErrorResponse {
  error: {
    code: string;
    message: string;
    field?: string;
  };
  meta: {
    request_id: string;
    timestamp: string;
  };
}

export interface PaginatedResponse<T> {
  data: T[];
  count: number;
  page?: number;
  page_size?: number;
  total_pages?: number;
}

export interface ImportResponse {
  data: {
    import_id: string;
    region: string;
    stations_imported: number;
    stations_updated: number;
    stations_failed: number;
    status: 'pending' | 'running' | 'completed' | 'failed';
  };
  meta: {
    request_id: string;
    timestamp: string;
  };
}

export interface BoundingBox {
  min_lat: number;
  min_lon: number;
  max_lat: number;
  max_lon: number;
}
