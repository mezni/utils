import type { BaseEntity } from './common';

export interface Charger extends BaseEntity {
  station_id: string;
  power_rating: number;
  deleted_at: string | null;
}

export interface CreateChargerRequest {
  station_id: string;
  status: string;
  power_rating: number;
}
