import type {
  AdminPartnerDto,
  CreatePartnerRequest,
  UpdatePartnerRequest,
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
  search?: string;
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

export async function listPartners(
  params?: ListParams,
): Promise<PaginatedResponse<AdminPartnerDto>> {
  const qs = new URLSearchParams();
  if (params?.page) qs.set("page", String(params.page));
  if (params?.per_page) qs.set("per_page", String(params.per_page));
  if (params?.search) qs.set("search", params.search);
  const q = qs.toString();
  return request<PaginatedResponse<AdminPartnerDto>>(
    `/api/v1/partners${q ? `?${q}` : ""}`,
  );
}

export async function getPartner(id: string): Promise<AdminPartnerDto> {
  return request<AdminPartnerDto>(`/api/v1/partners/${encodeURIComponent(id)}`);
}

export async function createPartner(
  data: CreatePartnerRequest,
): Promise<AdminPartnerDto> {
  return request<AdminPartnerDto>("/api/v1/partners", {
    method: "POST",
    body: JSON.stringify(data),
  });
}

export async function updatePartner(
  id: string,
  data: UpdatePartnerRequest,
): Promise<AdminPartnerDto> {
  return request<AdminPartnerDto>(
    `/api/v1/partners/${encodeURIComponent(id)}`,
    {
      method: "PUT",
      body: JSON.stringify(data),
    },
  );
}

export async function deletePartner(id: string): Promise<void> {
  return request<void>(
    `/api/v1/partners/${encodeURIComponent(id)}`,
    { method: "DELETE" },
  );
}
