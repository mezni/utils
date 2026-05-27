import axios from 'axios';

const API_URL = process.env.EXPO_PUBLIC_API_URL || 'http://127.0.0.1:8080/api/v1';

export const fetchNearbyStations = async (lat, lng) => {
  try {
    const response = await axios.get(`${API_URL}/stations/nearby`, {
      params: { lat, lng }
    });
    return response.data;
  } catch (error) {
    console.error("Link execution failure against api-service target:", error.message);
    throw error;
  }
};
