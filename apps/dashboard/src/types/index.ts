export interface Partner {
  id: string
  name: string
  stationCount: number
  status: 'active' | 'inactive' | 'pending'
  createdAt: string
}

export interface Station {
  id: string
  name: string
  address: string
  latitude: number
  longitude: number
  partnerId: string
  chargerCount: number
  status: 'available' | 'in-use' | 'maintenance'
  availability: number
  reviews: number
  averageRating: number
}

export interface Charger {
  id: string
  stationId: string
  connectorType: 'Type2' | 'CCS' | 'CHAdeMO' | 'Tesla'
  powerRating: number
  status: 'available' | 'in-use' | 'offline' | 'maintenance'
}

export interface User {
  id: string
  name: string
  email: string
  role: UserRole
  status: 'active' | 'inactive' | 'suspended'
  partnerId?: string
  createdAt: string
}

export interface Review {
  id: string
  stationId: string
  userId: string
  rating: number
  text: string
  date: string
  language: 'ar' | 'fr' | 'en'
}

export interface Report {
  id: string
  label: string
  value: number
  trend?: 'up' | 'down' | 'neutral'
  trendValue?: number
}

export type UserRole = 'partner' | 'admin' | 'registered_driver'