import type {
  AdminStationDto,
  CreateStationRequest,
  UpdateStationRequest,
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
  partner_id?: string;
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

export async function listStations(
  params?: ListParams,
): Promise<PaginatedResponse<AdminStationDto>> {
  const qs = new URLSearchParams();
  if (params?.page) qs.set("page", String(params.page));
  if (params?.per_page) qs.set("per_page", String(params.per_page));
  if (params?.partner_id) qs.set("partner_id", params.partner_id);
  const q = qs.toString();
  return request<PaginatedResponse<AdminStationDto>>(
    `/api/v1/stations${q ? `?${q}` : ""}`,
  );
}

export async function getStation(id: string): Promise<AdminStationDto> {
  return request<AdminStationDto>(`/api/v1/stations/${encodeURIComponent(id)}`);
}

export async function createStation(
  data: CreateStationRequest,
): Promise<AdminStationDto> {
  return request<AdminStationDto>("/api/v1/stations", {
    method: "POST",
    body: JSON.stringify(data),
  });
}

export async function updateStation(
  id: string,
  data: UpdateStationRequest,
): Promise<AdminStationDto> {
  return request<AdminStationDto>(
    `/api/v1/stations/${encodeURIComponent(id)}`,
    {
      method: "PUT",
      body: JSON.stringify(data),
    },
  );
}

export async function deleteStation(id: string): Promise<void> {
  return request<void>(
    `/api/v1/stations/${encodeURIComponent(id)}`,
    { method: "DELETE" },
  );
}
