import * as Location from 'expo-location'

export interface UserLocation {
  lat: number
  lng: number
}

export async function requestGeolocationPermission(): Promise<boolean> {
  try {
    const { status } = await Location.requestForegroundPermissionsAsync()
    return status === 'granted'
  } catch (error) {
    console.error('Geolocation permission error:', error)
    return false
  }
}

export async function getCurrentLocation(): Promise<UserLocation> {
  const { coords } = await Location.getCurrentPositionAsync({
    accuracy: Location.Accuracy.High,
  })

  return {
    lat: coords.latitude,
    lng: coords.longitude,
  }
}

export function initializeGeolocation() {
  console.log('Geolocation service initialized')
}

export function handleLocationError(error: any) {
  console.error('Geolocation error:', error)

  switch (error.code) {
    case 1:
      return {
        error: 'Permission denied',
        message: 'Geolocation permission was denied. Please enable it in your device settings.',
        canShowSystemSetting: true,
      }
    case 2:
      return {
        error: 'Location unavailable',
        message: 'Unable to determine your location. Please try again later.',
        canShowSystemSetting: false,
      }
    case 3:
      return {
        error: 'Request timed out',
        message: 'Location request timed out. Please check your connection and try again.',
        canShowSystemSetting: false,
      }
    default:
      return {
        error: 'Unknown error',
        message: 'An unknown error occurred. Please try again later.',
        canShowSystemSetting: false,
      }
  }
}