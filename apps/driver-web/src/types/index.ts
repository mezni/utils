export interface Station {
  id: string
  name: string
  address: string
  coordinates: { lat: number; lng: number }
  distance: number
  chargerCount: number
  availableCount: number
  availability: 'available' | 'unavailable'
  rating: number
  reviewCount: number
  imageUrl: string
}

export interface Charger {
  id: string
  stationId: string
  connectorType: 'Type2' | 'CCS' | 'CHAdeMO'
  powerKw: number
  availability: 'available' | 'unavailable'
  pricePerKwh: number
  lastMaintained: string
}

export interface Review {
  id: string
  stationId: string
  authorName: string
  rating: number
  text: string
  date: string
  language: 'ar' | 'fr' | 'en'
}

export interface DriverUser {
  id: string
  name: string
  email: string
  phone: string
  avatarUrl: string
  favoriteStationIds: string[]
  language: 'ar' | 'fr' | 'en'
}

export interface FilterState {
  chargerType: 'all' | 'Type2' | 'CCS' | 'CHAdeMO'
  availability: 'all' | 'available'
  searchQuery: string
}
