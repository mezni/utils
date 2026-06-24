import type { BaseEntity, TelemetrySnapshot } from './common';

export interface Station extends BaseEntity {
  partner_id: string;
  name: string;
  external_id: string;
  location: string;
  latitude: number;
  longitude: number;
  address: string;
  timezone: string;
  grid_limit_kw: number;
  deployed_at: string;
  deleted_at: string | null;

  /* aggregated */
  charger_count: number;
  chargers_active: number;
  total_power_kw: number;
  current_load_kw: number;
  telemetry: TelemetrySnapshot;
}

export interface CreateStationRequest {
  partner_id: string;
  name: string;
  location: string;
  latitude: number;
  longitude: number;
  address: string;
  timezone: string;
  grid_limit_kw: number;
}

export interface UpdateStationRequest {
  name?: string;
  location?: string;
  latitude?: number;
  longitude?: number;
  address?: string;
  timezone?: string;
  grid_limit_kw?: number;
}
