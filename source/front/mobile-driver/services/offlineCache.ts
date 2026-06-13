import AsyncStorage from '@react-native-async-storage/async-storage'

const CACHE_KEY = 'bornemap_stations_cache'
const CACHE_TIMESTAMP_KEY = 'bornemap_cache_timestamp'
const CACHE_EXPIRY = 5 * 60 * 1000 // 5 minutes

export interface CachedStation {
  data: any[]
  timestamp: number
}

export async function getCachedStations(): Promise<CachedStation | null> {
  try {
    const cached = await AsyncStorage.getItem(CACHE_KEY)
    if (!cached) {
      return null
    }

    const parsed: CachedStation = JSON.parse(cached)
    const timestamp = await AsyncStorage.getItem(CACHE_TIMESTAMP_KEY)

    if (timestamp && Date.now() - parseInt(timestamp) > CACHE_EXPIRY) {
      // Cache is expired, clear it
      await clearCachedStations()
      return null
    }

    return parsed
  } catch (error) {
    console.error('Error reading cache:', error)
    return null
  }
}

export async function cacheStations(stations: any[]): Promise<void> {
  try {
    const cacheData: CachedStation = {
      data: stations,
      timestamp: Date.now(),
    }

    await AsyncStorage.setItem(CACHE_KEY, JSON.stringify(cacheData))
    await AsyncStorage.setItem(CACHE_TIMESTAMP_KEY, cacheData.timestamp.toString())
  } catch (error) {
    console.error('Error writing cache:', error)
  }
}

export async function clearCachedStations(): Promise<void> {
  try {
    await AsyncStorage.removeItem(CACHE_KEY)
    await AsyncStorage.removeItem(CACHE_TIMESTAMP_KEY)
  } catch (error) {
    console.error('Error clearing cache:', error)
  }
}

export async function isCacheValid(): Promise<boolean> {
  try {
    const cached = await getCachedStations()
    return cached !== null
  } catch (error) {
    return false
  }
}

export async function refreshCache(): Promise<void> {
  await clearCachedStations()
}

// Cache expiration check
export async function checkCacheExpiry(): Promise<boolean> {
  const cached = await getCachedStations()
  return cached !== null && Date.now() - cached.timestamp < CACHE_EXPIRY
}

// Export constants
export const CACHE_EXPIRY_TIME = CACHE_EXPIRY
export const CACHE_KEY_STATIONS = CACHE_KEY
export const CACHE_KEY_TIMESTAMP = CACHE_TIMESTAMP_KEY
