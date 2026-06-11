export const config = {
  driverServiceUrl:
    process.env.EXPO_PUBLIC_DRIVER_API_URL ?? 'http://localhost:8080',
  clickstreamUrl:
    process.env.EXPO_PUBLIC_CLICKSTREAM_URL ?? 'http://localhost:8082',
};
