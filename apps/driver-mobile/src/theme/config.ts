export const config = {
  api: {
    baseUrl: import.meta.env.EXPO_PUBLIC_API_BASE_URL || 'https://api.example.tn',
    authUrl: import.meta.env.EXPO_PUBLIC_AUTH_BASE_URL || 'https://auth.example.tn',
  },
  keycloak: {
    realm: import.meta.env.EXPO_PUBLIC_REALM || 'bornemap',
    clientId: import.meta.env.EXPO_PUBLIC_CLIENT_ID || 'bornemap-driver-mobile',
    supportedLanguages: (import.meta.env.EXPO_PUBLIC_SUPPORTED_LANGUAGES || 'fr').split(','),
  },
  map: {
    defaultLat: parseFloat(import.meta.env.EXPO_PUBLIC_MAP_LAT || '36.8065'),
    defaultLng: parseFloat(import.meta.env.EXPO_PUBLIC_MAP_LNG || '10.1815'),
    defaultRadiusKm: parseInt(import.meta.env.EXPO_PUBLIC_MAP_DEFAULT_RADIUS_KM || '10'),
    maxRadiusKm: parseInt(import.meta.env.EXPO_PUBLIC_MAP_MAX_RADIUS_KM || '50'),
    defaultRadiusKm: parseInt(import.meta.env.EXPO_PUBLIC_MAP_DEFAULT_RADIUS_KM || '10'),
    maxRadiusKm: parseInt(import.meta.env.EXPO_PUBLIC_MAP_MAX_RADIUS_KM || '50'),
  },
  featureFlags: {
    enableReviews: import.meta.env.EXPO_PUBLIC_FF_ENABLE_REVIEWS === 'true',
    enableGisSync: import.meta.env.EXPO_PUBLIC_FF_ENABLE_GIS_SYNC === 'true',
    enableAnalytics: import.meta.env.EXPO_PUBLIC_FF_ENABLE_ANALYTICS === 'true',
  },
  offline: {
    cacheSizeMb: parseInt(import.meta.env.EXPO_PUBLIC_OFFLINE_CACHE_SIZE_MB || '100'),
  },
};
