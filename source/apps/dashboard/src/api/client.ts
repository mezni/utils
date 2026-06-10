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

export async function list<T>(resource: string, params?: Record<string, string>) {
  const qs = params ? '?' + new URLSearchParams(params).toString() : '';
  const res = await fetchWithError<unknown>(`/${resource}${qs}`);
  if (Array.isArray(res)) return res as T[];
  if (res && typeof res === 'object' && 'data' in res && Array.isArray((res as Record<string, unknown>).data)) {
    return (res as { data: T[] }).data;
  }
  throw { message: 'Unexpected API response format', status: 200 };
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
