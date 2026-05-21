import axios from 'axios';
import { API_BASE_URL } from '../config/api';

const api = axios.create({
  baseURL: API_BASE_URL,
  timeout: 10000,
  headers: {
    'Content-Type': 'application/json',
  },
});

export const getStations = async (lat, lng, radius) => {
  const response = await api.get('/api/v1/public/stations', {
    params: { lat, lng, radius },
  });
  return response.data;
};

export const getConfig = async () => {
  const response = await api.get('/api/v1/public/config');
  return response.data;
};

export const sendTelemetry = async (events) => {
  const response = await api.post('/api/v1/public/telemetry', { events });
  return response.data;
};

export default api;
