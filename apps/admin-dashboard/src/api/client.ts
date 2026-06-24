import axios from 'axios';

const API_VERSION = 'v1';

const apiClient = axios.create({
  baseURL: `/api/${API_VERSION}`,
  headers: { 'Content-Type': 'application/json' },
});

apiClient.interceptors.response.use(
  (response) => response,
  (error) => {
    const message = error.response?.data?.error?.message || error.message || 'Request failed';
    return Promise.reject(new Error(message));
  },
);

export default apiClient;
