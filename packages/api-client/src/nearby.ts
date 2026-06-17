import type { NearbyResponse, ErrorResponse, Location } from '@bornemap/shared-types';

const BASE_URL = process.env.API_BASE_URL || 'http://localhost:3001/api/v1';

interface NearbyParams {
  lat: number;
  lon: number;
  radius_m?: number;
  max_results?: number;
  visibility?: 'commercial' | 'private_home' | 'all';
}

export async function getNearby(params: NearbyParams): Promise<NearbyResponse | ErrorResponse> {
  const url = new URL(`${BASE_URL}/nearby`);
  
  url.searchParams.append('lat', params.lat.toString());
  url.searchParams.append('lon', params.lon.toString());
  
  if (params.radius_m) {
    url.searchParams.append('radius_m', params.radius_m.toString());
  }
  
  if (params.max_results) {
    url.searchParams.append('max_results', params.max_results.toString());
  }
  
  if (params.visibility) {
    url.searchParams.append('visibility', params.visibility);
  }
  
  const response = await fetch(url.toString(), {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
    },
  });

  const data = await response.json();

  if (!response.ok) {
    return {
      error: {
        code: data.error?.code || 'UNKNOWN_ERROR',
        message: data.error?.message || 'An error occurred',
        field: data.error?.field,
      },
      meta: {
        request_id: data.meta?.request_id || 'unknown',
        timestamp: new Date().toISOString(),
      },
    } as ErrorResponse;
  }

  return data;
}

export async function getNearbyWithAuth(params: NearbyParams, token: string): Promise<NearbyResponse | ErrorResponse> {
  const url = new URL(`${BASE_URL}/nearby`);
  
  url.searchParams.append('lat', params.lat.toString());
  url.searchParams.append('lon', params.lon.toString());
  
  if (params.radius_m) {
    url.searchParams.append('radius_m', params.radius_m.toString());
  }
  
  if (params.max_results) {
    url.searchParams.append('max_results', params.max_results.toString());
  }
  
  if (params.visibility) {
    url.searchParams.append('visibility', params.visibility);
  }
  
  const response = await fetch(url.toString(), {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${token}`,
    },
  });

  const data = await response.json();

  if (!response.ok) {
    return {
      error: {
        code: data.error?.code || 'UNKNOWN_ERROR',
        message: data.error?.message || 'An error occurred',
        field: data.error?.field,
      },
      meta: {
        request_id: data.meta?.request_id || 'unknown',
        timestamp: new Date().toISOString(),
      },
    } as ErrorResponse;
  }

  return data;
}
