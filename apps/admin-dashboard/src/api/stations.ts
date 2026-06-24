import apiClient from './client';
import type { ApiResponse, PaginatedResponse } from '../types/common';
import type { Station, CreateStationRequest } from '../types/station';

export async function listStations(page = 1, limit = 50, partnerId?: string): Promise<PaginatedResponse<Station>> {
  const { data } = await apiClient.get<ApiResponse<PaginatedResponse<Station>>>('/stations', {
    params: { page, limit, partner_id: partnerId },
  });
  return data.data;
}

export async function getStation(id: string): Promise<Station> {
  const { data } = await apiClient.get<ApiResponse<Station>>(`/stations/${id}`);
  return data.data;
}

export async function createStation(req: CreateStationRequest): Promise<Station> {
  const { data } = await apiClient.post<ApiResponse<Station>>('/stations', req);
  return data.data;
}

export async function updateStation(id: string, req: Partial<CreateStationRequest>): Promise<Station> {
  const { data } = await apiClient.put<ApiResponse<Station>>(`/stations/${id}`, req);
  return data.data;
}

export async function patchStation(id: string, name: string, location?: string): Promise<Station> {
  const { data } = await apiClient.patch<ApiResponse<Station>>(`/stations/${id}`, { name, location: location || null });
  return data.data;
}

export async function deleteStation(id: string): Promise<void> {
  await apiClient.delete(`/stations/${id}`);
}
