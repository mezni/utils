import apiClient from './client';
import type { ApiResponse, KpiData } from '../types/common';

export async function fetchKpis(): Promise<KpiData> {
  const { data } = await apiClient.get<ApiResponse<KpiData>>('/dashboard/kpis');
  return data.data;
}
