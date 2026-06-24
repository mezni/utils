import apiClient from './client';
import type { ApiResponse, PaginatedResponse } from '../types/common';
import type { Partner, CreatePartnerRequest } from '../types/partner';

export async function listPartners(page = 1, limit = 50): Promise<PaginatedResponse<Partner>> {
  const { data } = await apiClient.get<ApiResponse<PaginatedResponse<Partner>>>('/partners', {
    params: { page, limit },
  });
  return data.data;
}

export async function getPartner(id: string): Promise<Partner> {
  const { data } = await apiClient.get<ApiResponse<Partner>>(`/partners/${id}`);
  return data.data;
}

export async function createPartner(req: CreatePartnerRequest): Promise<Partner> {
  const { data } = await apiClient.post<ApiResponse<Partner>>('/partners', req);
  return data.data;
}

export async function updatePartner(id: string, req: Partial<CreatePartnerRequest>): Promise<Partner> {
  const { data } = await apiClient.put<ApiResponse<Partner>>(`/partners/${id}`, req);
  return data.data;
}

export async function patchPartner(id: string, name: string): Promise<Partner> {
  const { data } = await apiClient.patch<ApiResponse<Partner>>(`/partners/${id}`, { name });
  return data.data;
}

export async function deletePartner(id: string): Promise<void> {
  await apiClient.delete(`/partners/${id}`);
}
