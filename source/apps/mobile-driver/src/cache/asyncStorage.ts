import AsyncStorage from '@react-native-async-storage/async-storage'
import { Station, AsyncCacheEntry } from '../types'
import { roundTo2dp } from '../utils/coordinates'

const CACHE_PREFIX = '@bornemap/cache/'

function makeViewportKey(lat: number, lng: number): string {
  const roundedLat = roundTo2dp(lat)
  const roundedLng = roundTo2dp(lng)
  return `${CACHE_PREFIX}${roundedLat},${roundedLng}`
}

function makeLastKey(): string {
  return `${CACHE_PREFIX}last_viewport`
}

export async function writeCache(
  lat: number,
  lng: number,
  stations: Station[],
): Promise<void> {
  try {
    const roundedLat = roundTo2dp(lat)
    const roundedLng = roundTo2dp(lng)
    const viewportKey = `${roundedLat},${roundedLng}`

    const entry: AsyncCacheEntry = {
      viewportKey,
      stations,
      cachedAt: Date.now(),
      viewportCenter: { lat: roundedLat, lng: roundedLng },
    }

    await AsyncStorage.setItem(makeViewportKey(lat, lng), JSON.stringify(entry))
    await AsyncStorage.setItem(makeLastKey(), viewportKey)
  } catch {
    console.warn('AsyncStorage write failed — falling back to online-only')
  }
}

export async function readCache(
  lat: number,
  lng: number,
): Promise<Station[] | null> {
  try {
    const key = makeViewportKey(lat, lng)
    const raw = await AsyncStorage.getItem(key)

    if (!raw) {
      const lastKey = await AsyncStorage.getItem(makeLastKey())
      if (lastKey) {
        const lastRaw = await AsyncStorage.getItem(`${CACHE_PREFIX}${lastKey}`)
        if (lastRaw) {
          const entry: AsyncCacheEntry = JSON.parse(lastRaw)
          return entry.stations
        }
      }
      return null
    }

    const entry: AsyncCacheEntry = JSON.parse(raw)
    return entry.stations
  } catch {
    console.warn('AsyncStorage read failed — falling back to online-only')
    return null
  }
}

export async function clearCache(): Promise<void> {
  try {
    const keys = await AsyncStorage.getAllKeys()
    const cacheKeys = keys.filter((k) => k.startsWith(CACHE_PREFIX))
    await AsyncStorage.multiRemove(cacheKeys)
  } catch {
    console.warn('AsyncStorage clear failed')
  }
}
