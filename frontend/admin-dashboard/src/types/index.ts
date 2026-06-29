export * from "../api/partners";
export * from "../api/stations";
export * from "../api/connectors";

export interface ApiResponse<T> {
  data: T;
  message?: string;
}

export interface PaginatedResponse<T> {
  data: T[];
  total: number;
  page: number;
  limit: number;
  totalPages: number;
}

export interface PaginationParams {
  page?: number;
  limit?: number;
  search?: string;
}

export interface SortParams {
  field: string;
  order: "asc" | "desc";
}

export interface FilterParams {
  status?: string;
  dateRange?: {
    start: string;
    end: string;
  };
}
