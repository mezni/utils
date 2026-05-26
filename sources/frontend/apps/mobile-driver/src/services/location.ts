import * as Location from "expo-location"

export interface LocationResult {
  latitude: number
  longitude: number
  permissionGranted: boolean
}

export async function requestLocation(): Promise<LocationResult> {
  const { status } = await Location.requestForegroundPermissionsAsync()
  if (status !== "granted") {
    return { latitude: 0, longitude: 0, permissionGranted: false }
  }

  const loc = await Location.getCurrentPositionAsync({
    accuracy: Location.Accuracy.Balanced,
  })

  return {
    latitude: loc.coords.latitude,
    longitude: loc.coords.longitude,
    permissionGranted: true,
  }
}
