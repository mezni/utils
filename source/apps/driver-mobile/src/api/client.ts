import { Platform } from 'react-native';

const BASE_URL = Platform.select({
  ios: 'http://localhost:3001/api',
  android: 'http://10.0.2.2:3001/api',
  default: 'http://192.168.1.100:3001/api',
});

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
