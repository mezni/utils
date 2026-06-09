const BASE_URL = '/api';

export interface ApiError {
  message: string;
  status?: number;
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
    throw { message: body || res.statusText, status: res.status };
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

export function create<T>(resource: string, body: Partial<T>) {
  return fetchWithError<T>(`/${resource}`, {
    method: 'POST',
    body: JSON.stringify(body),
  });
}

export function update<T>(resource: string, id: string, body: Partial<T>) {
  return fetchWithError<T>(`/${resource}/${id}`, {
    method: 'PATCH',
    body: JSON.stringify(body),
  });
}

export function remove(resource: string, id: string) {
  return fetchWithError<void>(`/${resource}/${id}`, { method: 'DELETE' });
}
