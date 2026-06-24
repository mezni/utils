import type { Partner } from '../types/partner';
import type { Station } from '../types/station';
import type { Charger } from '../types/charger';
import type { ConnectorState, TelemetrySnapshot, EntityStatus, ChargerState } from '../types/common';

/* ─── Helpers ─── */

let idCounter = 1000;
const nid = (prefix: string) => `${prefix}-${(++idCounter).toString(36).toUpperCase().padStart(10, '0')}`;

const genTelemetry = (basePower: number): TelemetrySnapshot => ({
  power_kw: +(basePower * (0.3 + Math.random() * 0.7)).toFixed(2),
  voltage_v: 400 + Math.floor(Math.random() * 20),
  current_a: +(basePower * 1000 / 400 * (0.3 + Math.random() * 0.7)).toFixed(1),
  energy_total_kwh: +(Math.random() * 50000).toFixed(2),
  session_count: Math.floor(Math.random() * 5000),
  uptime_pct: +(95 + Math.random() * 5).toFixed(2),
  temperature_c: +(25 + Math.random() * 20).toFixed(1),
  last_seen: new Date(Date.now() - Math.random() * 60000).toISOString(),
});

const connectors: ConnectorState[] = [
  { id: 'A1', type: 'CCS2', status: 'ACTIVE', power_rated_kw: 350, power_current_kw: 0, session_active: false, vehicle_connected: false },
  { id: 'A2', type: 'CCS2', status: 'ACTIVE', power_rated_kw: 350, power_current_kw: 0, session_active: false, vehicle_connected: false },
  { id: 'B1', type: 'TYPE2', status: 'ACTIVE', power_rated_kw: 22, power_current_kw: 0, session_active: false, vehicle_connected: false },
];

const statuses: EntityStatus[] = ['ACTIVE', 'ACTIVE', 'ACTIVE', 'FAULTED', 'CHARGING', 'THROTTLED', 'MAINTENANCE', 'OFFLINE'];
const chargerStates: ChargerState[] = ['IDLE', 'CHARGING', 'FAULTED', 'THROTTLED', 'OFFLINE', 'MAINTENANCE'];
const pick = <T,>(arr: T[]) => arr[Math.floor(Math.random() * arr.length)];

/* ─── Partners ─── */

export const partners: Partner[] = [
  { id: 'PRT-A1B2C3D4E5F6', name: 'GreenCharge Networks', external_id: 'GCN-EU-001', tax_id: 'DE-321654987', email: 'ops@greencharge.io', phone: '+49 30 1234567', address: 'Alexanderplatz 1, 10178 Berlin', is_valid: true, status: 'ACTIVE', deleted_at: null, station_count: 12, charger_count: 48, total_power_kw: 16800, telemetry: genTelemetry(16800), created_at: '2024-01-15T08:00:00Z', updated_at: '2025-06-20T14:30:00Z' },
  { id: 'PRT-F6E5D4C3B2A1A', name: 'VoltVault Energy', external_id: 'VVE-NA-002', tax_id: 'US-98-7654321', email: 'support@voltvault.com', phone: '+1 415 555 0199', address: '500 Market St, San Francisco, CA 94105', is_valid: true, status: 'ACTIVE', deleted_at: null, station_count: 8, charger_count: 32, total_power_kw: 11200, telemetry: genTelemetry(11200), created_at: '2024-03-01T10:00:00Z', updated_at: '2025-06-22T09:15:00Z' },
  { id: 'PRT-7B8A9C0D1E2F', name: 'ChargePoint Europe BV', external_id: 'CPE-NL-003', tax_id: 'NL-854712369B01', email: 'ops@chargepoint-eu.com', phone: '+31 20 789 0123', address: 'Herengracht 250, 1016 BT Amsterdam', is_valid: true, status: 'ACTIVE', deleted_at: null, station_count: 25, charger_count: 120, total_power_kw: 42000, telemetry: genTelemetry(42000), created_at: '2023-11-20T07:30:00Z', updated_at: '2025-06-23T11:00:00Z' },
  { id: 'PRT-3F4E5D6C7B8A', name: 'ElectraDrive UK Ltd', external_id: 'EDK-GB-004', tax_id: 'GB-456789123', email: 'hello@electradrive.co.uk', phone: '+44 20 7946 0123', address: '1 Electric Ave, London EC2A 4NE', is_valid: true, status: 'FAULTED', deleted_at: null, station_count: 6, charger_count: 18, total_power_kw: 10800, telemetry: genTelemetry(10800), created_at: '2024-06-10T09:00:00Z', updated_at: '2025-06-21T16:45:00Z' },
  { id: 'PRT-9A8B7C6D5E4F', name: 'SunCharge Solar GmbH', external_id: 'SCS-DE-005', tax_id: 'DE-123987456', email: 'info@suncharge.de', phone: '+49 89 9876543', address: 'Marienplatz 8, 80331 München', is_valid: false, status: 'MAINTENANCE', deleted_at: null, station_count: 3, charger_count: 9, total_power_kw: 3600, telemetry: genTelemetry(3600), created_at: '2024-09-01T12:00:00Z', updated_at: '2025-06-10T08:00:00Z' },
  { id: 'PRT-2B3C4D5E6F7A', name: 'PowerGrid Mobility SE', external_id: 'PGM-FR-006', tax_id: 'FR-753951852', email: 'dispatch@powergrid-mobility.fr', phone: '+33 1 44 55 66 77', address: '10 Rue de Rivoli, 75001 Paris', is_valid: true, status: 'ACTIVE', deleted_at: null, station_count: 18, charger_count: 72, total_power_kw: 25200, telemetry: genTelemetry(25200), created_at: '2024-02-14T07:00:00Z', updated_at: '2025-06-23T06:00:00Z' },
  { id: 'PRT-5A6B7C8D9E0F', name: 'EcoFleet Charging AS', external_id: 'EFC-NO-007', tax_id: 'NO-987654321MVA', email: 'fleet@ecofleet.no', phone: '+47 22 33 44 55', address: 'Karl Johans gate 12, 0154 Oslo', is_valid: true, status: 'THROTTLED', deleted_at: null, station_count: 4, charger_count: 16, total_power_kw: 4800, telemetry: genTelemetry(4800), created_at: '2024-04-22T11:00:00Z', updated_at: '2025-06-19T13:00:00Z' },
  { id: 'PRT-1C2D3E4F5A6B', name: 'CityCharge Infrastructure', external_id: 'CCI-IT-008', tax_id: 'IT-12345678901', email: 'admin@citycharge.it', phone: '+39 06 1234 5678', address: 'Via del Corso 22, 00186 Roma', is_valid: true, status: 'ACTIVE', deleted_at: null, station_count: 10, charger_count: 40, total_power_kw: 14000, telemetry: genTelemetry(14000), created_at: '2024-07-01T08:00:00Z', updated_at: '2025-06-22T17:00:00Z' },
];

/* ─── Stations ─── */

const stationNames = [
  'Berlin Hauptbahnhof Hub', 'Potsdamer Platz Garage', 'TXL Airport Fast-Charge', 'Alexanderplatz Mall',
  'San Francisco Ferry Building', 'Oakland Downtown Garage', 'Mission District Hub',
  'Amsterdam Centraal', 'Rotterdam Port', 'Utrecht Science Park',
  'London King\'s Cross', 'Canary Wharf', 'Heathrow Airport',
  'Marienplatz Garage', 'Olympiapark', 'Munich Airport',
  'Paris Gare du Nord', 'Champs-Élysées', 'CDG Airport Terminal 2',
  'Oslo Sentrum', 'Bergen Harbour', 'Trondheim Hub',
  'Roma Termini', 'Colosseo Garage', 'Fiumicino Airport',
];

export const stations: Station[] = partners.flatMap((p, pi) =>
  Array.from({ length: Math.ceil(p.station_count / 3) }, (_, i) => {
    const idx = pi * 3 + i;
    const active = Math.floor(Math.random() * 4) + 2;
    return {
      id: `STA-${(9000 + idx).toString(36).toUpperCase().padStart(10, '0')}`,
      partner_id: p.id,
      name: stationNames[idx % stationNames.length],
      external_id: `ST-${(4000 + idx).toString(16).toUpperCase().padStart(6, '0')}`,
      location: `${48 + Math.random() * 10},${2 + Math.random() * 10}`,
      latitude: +(48 + Math.random() * 10).toFixed(6),
      longitude: +(2 + Math.random() * 10).toFixed(6),
      address: `${100 + idx} Example St, City ${idx}`,
      timezone: 'Europe/Berlin',
      grid_limit_kw: 500 + Math.floor(Math.random() * 1500),
      status: pick(statuses),
      deployed_at: '2024-01-01T00:00:00Z',
      deleted_at: null,
      charger_count: active + Math.floor(Math.random() * 2),
      chargers_active: active,
      total_power_kw: (active + Math.floor(Math.random() * 2)) * 350,
      current_load_kw: Math.floor(Math.random() * 1000),
      telemetry: genTelemetry(1000),
      created_at: '2024-01-01T00:00:00Z',
      updated_at: new Date(Date.now() - Math.random() * 86400000).toISOString(),
    };
  })
);

/* ─── Chargers ─── */

const manufacturers = ['ABB', 'Siemens', 'Delta Electronics', 'Alpitronic', 'Tesla', 'ChargePoint', 'Tritium', 'BTC Power'];
const models: Record<string, string[]> = {
  'ABB': ['Terra 350', 'Terra 184', 'Terra HP'],
  'Siemens': ['VersiCharge Ultra', 'Sicharge UC 150'],
  'Delta Electronics': ['UFC 200', 'UFC 350'],
  'Alpitronic': ['Hypercharger HYC400', 'Hypercharger HYC150'],
  'Tesla': ['V4 Supercharger', 'Wall Connector 3'],
  'ChargePoint': ['Express Plus', 'CPE250'],
  'Tritium': ['RTM 175', 'PKM 150'],
  'BTC Power': ['EVC-350', 'EVC-150'],
};

export const chargers: Charger[] = stations.flatMap((s) =>
  Array.from({ length: s.charger_count }, (_, i) => {
    const mf = pick(manufacturers);
    const chState = pick(chargerStates);
    const chargingNow = chState === 'CHARGING';
    const powerRated = [150, 175, 350, 400][Math.floor(Math.random() * 4)];
    return {
      id: `CHR-${(8000 + Math.floor(Math.random() * 90000)).toString(36).toUpperCase().padStart(10, '0')}`,
      station_id: s.id,
      external_id: `CH-${(5000 + Math.floor(Math.random() * 50000)).toString(16).toUpperCase().padStart(6, '0')}`,
      charge_box_id: `CP-${Date.now().toString(36).toUpperCase()}-${String(i).padStart(3, '0')}`,
      ocpp_version: Math.random() > 0.4 ? '2.0.1' : '1.6',
      firmware_version: `v${Math.floor(Math.random() * 10)}.${Math.floor(Math.random() * 5)}.${Math.floor(Math.random() * 20)}`,
      serial_number: `SN-${(9000 + Math.floor(Math.random() * 90000)).toString(36).toUpperCase()}`,
      model: pick(models[mf]),
      manufacturer: mf,
      power_rating_kw: powerRated,
      power_available_kw: chargingNow ? powerRated * 0.85 : powerRated,
      connectors: connectors.map(c => ({
        ...c,
        power_rated_kw: powerRated,
        power_current_kw: chargingNow ? powerRated * 0.7 * (1 + Math.random() * 0.3) : 0,
        session_active: chargingNow,
        vehicle_connected: chargingNow || Math.random() > 0.7,
        status: chargingNow ? 'CHARGING' : 'ACTIVE',
      })),
      max_connectors: 3,
      ocpi_visible: Math.random() > 0.2,
      revenue_share_pct: +(Math.random() * 15).toFixed(2),
      payout_address: `0x${Array.from({ length: 40 }, () => Math.floor(Math.random() * 16).toString(16)).join('')}`,
      tariff_id: `TARIFF-${(2000 + Math.floor(Math.random() * 8000)).toString(36).toUpperCase()}`,
      energy_rate_per_kwh: +(0.25 + Math.random() * 0.45).toFixed(3),
      charger_state: chState,
      session_id: chargingNow ? `SESS-${Date.now().toString(36).toUpperCase()}` : null,
      session_started_at: chargingNow ? new Date(Date.now() - Math.random() * 3600000).toISOString() : null,
      session_energy_kwh: chargingNow ? +(Math.random() * 50).toFixed(2) : 0,
      status: chState === 'FAULTED' ? 'FAULTED' : chState === 'OFFLINE' ? 'OFFLINE' : chState === 'MAINTENANCE' ? 'MAINTENANCE' : 'ACTIVE',
      deleted_at: null,
      unbound_at: null,
      telemetry: genTelemetry(powerRated),
      created_at: '2024-01-01T00:00:00Z',
      updated_at: new Date(Date.now() - Math.random() * 3600000).toISOString(),
    };
  })
);

/* ─── Data Access Helpers ─── */

export const getPartners = (): Partner[] => partners;
export const getPartner = (id: string): Partner | undefined => partners.find(p => p.id === id);

export const getStationsForPartner = (partnerId: string): Station[] =>
  stations.filter(s => s.partner_id === partnerId);

export const getStation = (id: string): Station | undefined => stations.find(s => s.id === id);

export const getChargersForStation = (stationId: string): Charger[] =>
  chargers.filter(c => c.station_id === stationId);

export const getCharger = (id: string): Charger | undefined => chargers.find(c => c.id === id);

/* ─── Mutations (in-memory) ─── */

export const createPartner = (req: { name: string }): Partner => {
  const p: Partner = {
    id: nid('PRT'),
    name: req.name,
    external_id: `PRT-${Date.now().toString(36).toUpperCase()}`,
    tax_id: '',
    email: '',
    phone: '',
    address: '',
    is_valid: true,
    status: 'ACTIVE',
    deleted_at: null,
    station_count: 0,
    charger_count: 0,
    total_power_kw: 0,
    telemetry: genTelemetry(0),
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  };
  partners.unshift(p);
  return p;
};

export const updatePartner = (id: string, data: Record<string, unknown>): Partner | undefined => {
  const p = getPartner(id);
  if (!p) return;
  Object.assign(p, data, { updated_at: new Date().toISOString() });
  return p;
};

export const deletePartner = (id: string): boolean => {
  const idx = partners.findIndex(p => p.id === id);
  if (idx === -1) return false;
  const hasActive = stations.some(s => s.partner_id === id && s.status === 'ACTIVE');
  if (hasActive) return false;
  partners.splice(idx, 1);
  return true;
};

export const createStation = (req: { name: string; location: string; partner_id: string }): Station => {
  const s: Station = {
    id: nid('STA'),
    partner_id: req.partner_id,
    name: req.name,
    external_id: `STA-${Date.now().toString(36).toUpperCase()}`,
    location: req.location,
    latitude: 51.5,
    longitude: -0.12,
    address: '',
    timezone: 'Europe/London',
    grid_limit_kw: 1000,
    status: 'ACTIVE',
    deployed_at: new Date().toISOString(),
    deleted_at: null,
    charger_count: 0,
    chargers_active: 0,
    total_power_kw: 0,
    current_load_kw: 0,
    telemetry: genTelemetry(0),
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  };
  stations.unshift(s);
  return s;
};

export const updateStation = (id: string, data: Record<string, unknown>): Station | undefined => {
  const s = getStation(id);
  if (!s) return;
  Object.assign(s, data, { updated_at: new Date().toISOString() });
  return s;
};

export const deleteStation = (id: string): boolean => {
  const idx = stations.findIndex(s => s.id === id);
  if (idx === -1) return false;
  const hasActive = chargers.some(c => c.station_id === id && c.status === 'ACTIVE');
  if (hasActive) return false;
  stations.splice(idx, 1);
  return true;
};

export const createCharger = (req: { station_id: string; charge_box_id: string }): Charger => {
  const c: Charger = {
    id: nid('CHR'),
    station_id: req.station_id,
    external_id: `CHR-${Date.now().toString(36).toUpperCase()}`,
    charge_box_id: req.charge_box_id,
    ocpp_version: '2.0.1',
    firmware_version: 'v1.0.0',
    serial_number: `SN-${Date.now().toString(36).toUpperCase()}`,
    model: 'Terra 350',
    manufacturer: 'ABB',
    power_rating_kw: 350,
    power_available_kw: 350,
    connectors: connectors.map(c => ({ ...c })),
    max_connectors: 3,
    ocpi_visible: true,
    revenue_share_pct: 5.0,
    payout_address: '',
    tariff_id: 'TARIFF-STANDARD',
    energy_rate_per_kwh: 0.35,
    charger_state: 'IDLE',
    session_id: null,
    session_started_at: null,
    session_energy_kwh: 0,
    status: 'ACTIVE',
    deleted_at: null,
    unbound_at: null,
    telemetry: genTelemetry(350),
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  };
  chargers.unshift(c);
  return c;
};

export const updateCharger = (id: string, data: Record<string, unknown>): Charger | undefined => {
  const c = getCharger(id);
  if (!c) return;
  Object.assign(c, data, { updated_at: new Date().toISOString() });
  return c;
};

export const unbindCharger = (id: string): Charger | undefined => {
  const c = getCharger(id);
  if (!c) return;
  c.status = 'DISABLED';
  c.ocpi_visible = false;
  c.unbound_at = new Date().toISOString();
  c.updated_at = new Date().toISOString();
  return c;
};

export const softDeleteCharger = (id: string): boolean => {
  const c = getCharger(id);
  if (!c) return false;
  c.status = 'DISABLED';
  c.deleted_at = new Date().toISOString();
  c.updated_at = new Date().toISOString();
  return true;
};
