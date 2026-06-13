export interface CachedStation {
  id: string
  data: any[]
  timestamp: number
  expiry: number
}

const CACHE_KEY = 'bornemap_web_stations_cache'
const CACHE_EXPIRY = 5 * 60 * 1000 // 5 minutes

export class WebOfflineCache {
  private cache: Map<string, CachedStation> = new Map()
  private listeners: Set<(stations: any[]) => void> = new Set()

  constructor() {
    // Load cache from localStorage on initialization
    this.loadCacheFromStorage()
  }

  private loadCacheFromStorage(): void {
    try {
      const cached = localStorage.getItem(CACHE_KEY)
      if (cached) {
        const parsed: { [key: string]: CachedStation } = JSON.parse(cached)

        // Convert to Map and remove expired entries
        Object.entries(parsed).forEach(([key, value]) => {
          if (Date.now() - value.timestamp < value.expiry) {
            this.cache.set(key, value)
          }
        })
      }
    } catch (error) {
      console.error('Error loading cache from storage:', error)
    }
  }

  private saveCacheToStorage(): void {
    try {
      const cacheData: { [key: string]: CachedStation } = Object.fromEntries(this.cache.entries())
      localStorage.setItem(CACHE_KEY, JSON.stringify(cacheData))
    } catch (error) {
      console.error('Error saving cache to storage:', error)
    }
  }

  public async get(key: string): Promise<any[] | null> {
    const cached = this.cache.get(key)

    if (!cached) {
      return null
    }

    // Check if cache is expired
    if (Date.now() - cached.timestamp > cached.expiry) {
      this.cache.delete(key)
      this.saveCacheToStorage()
      return null
    }

    return cached.data
  }

  public async set(key: string, data: any[]): Promise<void> {
    const cacheData: CachedStation = {
      id: key,
      data,
      timestamp: Date.now(),
      expiry: CACHE_EXPIRY,
    }

    this.cache.set(key, cacheData)
    this.saveCacheToStorage()

    // Notify listeners
    this.listeners.forEach(listener => listener(data))
  }

  public async clear(key?: string): Promise<void> {
    if (key) {
      this.cache.delete(key)
    } else {
      this.cache.clear()
    }
    this.saveCacheToStorage()
  }

  public async has(key: string): Promise<boolean> {
    const cached = this.cache.get(key)
    if (!cached) {
      return false
    }

    return Date.now() - cached.timestamp < cached.expiry
  }

  public async refresh(key: string): Promise<void> {
    this.cache.delete(key)
    this.saveCacheToStorage()
  }

  public subscribe(listener: (stations: any[]) => void): () => void {
    this.listeners.add(listener)

    return () => {
      this.listeners.delete(listener)
    }
  }

  public async getAllKeys(): Promise<string[]> {
    return Array.from(this.cache.keys())
  }

  public async getCacheSize(): Promise<number> {
    let totalSize = 0

    this.cache.forEach((value) => {
      const jsonString = JSON.stringify(value)
      totalSize += jsonString.length
    })

    return totalSize
  }
}

// Create singleton instance
export const webCache = new WebOfflineCache()

// Export cache key constants
export const CACHE_KEYS = {
  STATIONS: 'stations',
  STATION_DETAILS: 'station_details',
} as const
