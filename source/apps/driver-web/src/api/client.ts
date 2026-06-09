const BASE_URL = '/api';

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
  status: string;
}

export interface VisibleStation extends Station {
  availableCount: number;
  totalChargers: number;
}

export async function fetchWithError<T>(
  url: string,
  options?: RequestInit
): Promise<T> {
  const res = await fetch(`${BASE_URL}${url}`, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(body || res.statusText);
  }
  if (res.status === 204) return undefined as T;
  return res.json();
}

export function list<T>(resource: string, params?: Record<string, string>) {
  const qs = params ? '?' + new URLSearchParams(params).toString() : '';
  return fetchWithError<T[]>(`/${resource}${qs}`);
}

export function get<T>(resource: string, id: string) {
  return fetchWithError<T>(`/${resource}/${id}`);
}
