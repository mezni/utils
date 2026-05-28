import axios from 'axios';
import { Platform } from 'react-native';

function getBaseUrl() {
  if (process.env.EXPO_PUBLIC_API_URL) return process.env.EXPO_PUBLIC_API_URL;
  if (Platform.OS === 'android') return 'http://10.0.2.2:8080/api/v1';
  return 'http://localhost:8080/api/v1';
}

const API_BASE_URL = getBaseUrl();

const STATIONS_ENDPOINT = `${API_BASE_URL}/stations/nearby`;

console.log(`📡 API base URL: ${API_BASE_URL}`);

export const fetchNearbyStations = async ({ lat, lng, showStaged = false, signal }) => {
  try {
    const response = await axios.get(STATIONS_ENDPOINT, {
      params: { lat, lng, show_staged: showStaged },
      timeout: 5000,
      signal,
    });
    return response.data;
  } catch (error) {
    if (axios.isCancel(error)) return;
    console.error(`🚨 API Service integration handshake failure: ${error.message}`);
    console.warn(`🛠️  Verify api-service is running and reachable at: ${STATIONS_ENDPOINT}`);
    console.warn(`💡 Set EXPO_PUBLIC_API_URL to override (e.g., http://<YOUR_LAN_IP>:8080/api/v1)`);
    throw error;
  }
};
