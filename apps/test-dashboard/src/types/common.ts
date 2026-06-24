/* ─── Core Entity Types ─── */

export type EntityStatus = 'ACTIVE' | 'FAULTED' | 'THROTTLED' | 'CHARGING' | 'OFFLINE' | 'MAINTENANCE' | 'DISABLED';

export interface BaseEntity {
  id: string;
  status: EntityStatus;
  created_at: string;
  updated_at: string;
}

export interface PaginatedResponse<T> {
  items: T[];
  total: number;
  page: number;
  pages: number;
}

/* ─── Telemetry & Metrics ─── */

export interface TelemetrySnapshot {
  power_kw: number;
  voltage_v: number;
  current_a: number;
  energy_total_kwh: number;
  session_count: number;
  uptime_pct: number;
  temperature_c: number;
  last_seen: string;
}

export interface ConnectorState {
  id: string;
  type: 'CCS2' | 'CHADEMO' | 'TYPE2' | 'GBT' | 'NACS';
  status: EntityStatus;
  power_rated_kw: number;
  power_current_kw: number;
  session_active: boolean;
  vehicle_connected: boolean;
}

/* ─── Charger-Specific State ─── */

export type ChargerState = 'IDLE' | 'CHARGING' | 'FAULTED' | 'THROTTLED' | 'OFFLINE' | 'MAINTENANCE';

export type ConnectorType = 'CCS2' | 'CHADEMO' | 'TYPE2' | 'GBT' | 'NACS';
