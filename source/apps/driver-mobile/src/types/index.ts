export interface Partner {
  id: string;
  name: string;
  is_verified: boolean;
  is_live: boolean;
  is_active: boolean;
}

export interface Station {
  id: string;
  partner_id: string;
  name: string;
  address: string;
  latitude: number;
  longitude: number;
}

export interface Charger {
  id: string;
  station_id: string;
  connector_type: string;
  power_kw: number;
  status: 'available' | 'in_use' | 'maintenance' | 'offline';
}

export interface VisibleStation extends Station {
  availableCount: number;
  totalChargers: number;
}
