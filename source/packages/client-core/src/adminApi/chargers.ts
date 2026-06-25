import type {
  AdminChargerDto,
  CreateChargerRequest,
  UpdateChargerRequest,
} from "@bornemap/domain-types";

export interface PaginationInfo {
  page: number;
  per_page: number;
  total: number;
  total_pages: number;
}

export interface PaginatedResponse<T> {
  data: T[];
  pagination: PaginationInfo;
}

interface ListParams {
  page?: number;
  per_page?: number;
  station_id?: string;
}

const DEFAULT_BASE = "";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${DEFAULT_BASE}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.error || `API error: ${res.status} ${res.statusText}`);
  }
  if (res.status === 204) return undefined as T;
  return res.json();
}

export async function listChargers(
  params?: ListParams,
): Promise<PaginatedResponse<AdminChargerDto>> {
  const qs = new URLSearchParams();
  if (params?.page) qs.set("page", String(params.page));
  if (params?.per_page) qs.set("per_page", String(params.per_page));
  if (params?.station_id) qs.set("station_id", params.station_id);
  const q = qs.toString();
  return request<PaginatedResponse<AdminChargerDto>>(
    `/api/v1/chargers${q ? `?${q}` : ""}`,
  );
}

export async function getCharger(id: string): Promise<AdminChargerDto> {
  return request<AdminChargerDto>(`/api/v1/chargers/${encodeURIComponent(id)}`);
}

export async function createCharger(
  data: CreateChargerRequest,
): Promise<AdminChargerDto> {
  return request<AdminChargerDto>("/api/v1/chargers", {
    method: "POST",
    body: JSON.stringify(data),
  });
}

export async function updateCharger(
  id: string,
  data: UpdateChargerRequest,
): Promise<AdminChargerDto> {
  return request<AdminChargerDto>(
    `/api/v1/chargers/${encodeURIComponent(id)}`,
    {
      method: "PUT",
      body: JSON.stringify(data),
    },
  );
}

export async function deleteCharger(id: string): Promise<void> {
  return request<void>(
    `/api/v1/chargers/${encodeURIComponent(id)}`,
    { method: "DELETE" },
  );
}
