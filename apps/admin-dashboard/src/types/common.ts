export interface Pagination {
  page: number;
  limit: number;
  total: number;
  pages: number;
}

export interface ApiResponse<T> {
  success: boolean;
  data: T;
  error: null;
}

export interface ErrorResponse {
  success: boolean;
  data: null;
  error: ErrorDetail;
}

export interface ErrorDetail {
  code: string;
  message: string;
}

export interface KpiData {
  partners_count: number;
  stations_count: number;
  chargers_count: number;
}

export type EntityStatus = 'ACTIVE' | 'INACTIVE' | 'MAINTENANCE' | 'DISABLED';

export interface BaseEntity {
  id: string;
  status: EntityStatus;
  created_by: string;
  updated_by: string;
  created_at: string;
  updated_at: string;
}
