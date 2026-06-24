import apiClient from './client';
import type { ApiResponse, PaginatedResponse } from '../types/common';
import type { Charger, CreateChargerRequest } from '../types/charger';

export async function listChargers(page = 1, limit = 50, stationId?: string): Promise<PaginatedResponse<Charger>> {
  const { data } = await apiClient.get<ApiResponse<PaginatedResponse<Charger>>>('/chargers', {
    params: { page, limit, station_id: stationId },
  });
  return data.data;
}

export async function getCharger(id: string): Promise<Charger> {
  const { data } = await apiClient.get<ApiResponse<Charger>>(`/chargers/${id}`);
  return data.data;
}

export async function createCharger(req: CreateChargerRequest): Promise<Charger> {
  const { data } = await apiClient.post<ApiResponse<Charger>>('/chargers', req);
  return data.data;
}

export async function updateCharger(id: string, req: Partial<CreateChargerRequest>): Promise<Charger> {
  const { data } = await apiClient.put<ApiResponse<Charger>>(`/chargers/${id}`, req);
  return data.data;
}

export async function patchCharger(id: string, powerRating: number): Promise<Charger> {
  const { data } = await apiClient.patch<ApiResponse<Charger>>(`/chargers/${id}`, { power_rating: powerRating });
  return data.data;
}

export async function deleteCharger(id: string): Promise<void> {
  await apiClient.delete(`/chargers/${id}`);
}
