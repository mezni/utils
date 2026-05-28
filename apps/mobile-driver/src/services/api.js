import axios from 'axios';

const API_BASE_URL = process.env.EXPO_PUBLIC_API_URL || 'http://127.0.0.1:8080/api/v1';

export const fetchNearbyStations = async () => {
  try {
    const response = await axios.get(`${API_BASE_URL}/stations/nearby`);
    return response.data;
  } catch (error) {
    console.error("🚨 API Service integration handshake failure:", error.message);
    throw error;
  }
};
