import type { BaseEntity, ConnectorState, TelemetrySnapshot, ChargerState } from './common';

export interface Charger extends BaseEntity {
  station_id: string;
  external_id: string;
  charge_box_id: string;
  ocpp_version: '1.6' | '2.0.1';
  firmware_version: string;
  serial_number: string;
  model: string;
  manufacturer: string;

  /* power */
  power_rating_kw: number;
  power_available_kw: number;

  /* hardware */
  connectors: ConnectorState[];
  max_connectors: number;

  /* OCPI roaming */
  ocpi_visible: boolean;

  /* financial */
  revenue_share_pct: number;
  payout_address: string;
  tariff_id: string;
  energy_rate_per_kwh: number;

  /* state */
  charger_state: ChargerState;
  session_id: string | null;
  session_started_at: string | null;
  session_energy_kwh: number;

  /* safety */
  deleted_at: string | null;
  unbound_at: string | null;

  telemetry: TelemetrySnapshot;
}

export interface CreateChargerRequest {
  station_id: string;
  charge_box_id: string;
  ocpp_version: '1.6' | '2.0.1';
  power_rating_kw: number;
  model: string;
  manufacturer: string;
  serial_number: string;
  max_connectors: number;
}

export interface UpdateChargerRequest {
  power_rating_kw?: number;
  firmware_version?: string;
  ocpi_visible?: boolean;
  revenue_share_pct?: number;
  payout_address?: string;
  tariff_id?: string;
  energy_rate_per_kwh?: number;
}
