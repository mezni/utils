import type { BaseEntity, TelemetrySnapshot } from './common';

export interface Partner extends BaseEntity {
  name: string;
  external_id: string;
  tax_id: string;
  email: string;
  phone: string;
  address: string;
  is_valid: boolean;
  deleted_at: string | null;

  /* aggregated */
  station_count: number;
  charger_count: number;
  total_power_kw: number;
  telemetry: TelemetrySnapshot;
}

export interface CreatePartnerRequest {
  name: string;
  email: string;
  phone: string;
  address: string;
  tax_id: string;
}

export interface UpdatePartnerRequest {
  name?: string;
  email?: string;
  phone?: string;
  address?: string;
  tax_id?: string;
}
