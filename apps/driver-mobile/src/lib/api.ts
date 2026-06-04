import { ApiClient } from '@bornemap/api-client';
import * as auth from '@bornemap/auth-client';
import config from '../theme/config';

const GATEWAY_BASE_URL = (import.meta.env.VITE_GATEWAY_BASE_URL as string) ?? 'http://localhost';
const API_BASE_URL = `${GATEWAY_BASE_URL}/api/v1/driver`;

export const apiClient = new ApiClient({
  baseUrl: API_BASE_URL,
  getToken: async () => {
    try {
      const token = await auth.getToken();
      return token;
    } catch (error) {
      console.error('Failed to get auth token:', error);
      return null;
    }
  },
});
