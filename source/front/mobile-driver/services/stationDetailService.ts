export interface Charger {
  id: string
  station_id: string
  charger_type: 'CCS' | 'CHAdeMO' | 'AC'
  connector_count: number
  availability_status: 'available' | 'in_use' | 'maintenance'
  power_kw: number
  is_active: boolean
  created_at: string
  updated_at: string
}

export interface StationImage {
  id: string
  station_id: string
  url: string
  caption: string
  is_primary: boolean
  created_at: string
}

export interface Station {
  id: string
  name: string
  address: string
  geometry: {
    type: 'Point'
    coordinates: [number, number]
  }
  amenities: string[]
  operating_hours: string
  created_at: string
  updated_at: string
  chargers: Charger[]
  images: StationImage[]
}

export interface StationDetailParams {
  id: string
}

export async function fetchStationDetailById(id: string): Promise<Station> {
  // This is the actual implementation that will be replaced with real API calls
  // For now, return mock data

  // Simulate network delay
  await new Promise<void>(resolve => setTimeout(() => resolve(), 300))

  // Generate station images
  const images: StationImage[] = [
    {
      id: `IMG-${Math.random().toString(36).substring(2, 11)}`,
      station_id: id,
      url: 'https://images.unsplash.com/photo-1559526324-4b87b5e36e44?w=800&h=600&fit=crop',
      caption: 'Main entrance',
      is_primary: true,
      created_at: new Date().toISOString(),
    },
    {
      id: `IMG-${Math.random().toString(36).substring(2, 11)}`,
      station_id: id,
      url: 'https://images.unsplash.com/photo-1590674899484-d5640e854abe?w=800&h=600&fit=crop',
      caption: 'Charging bays',
      is_primary: false,
      created_at: new Date().toISOString(),
    },
    {
      id: `IMG-${Math.random().toString(36).substring(2, 11)}`,
      station_id: id,
      url: 'https://images.unsplash.com/photo-1565514020170-44e1f781c6cd?w=800&h=600&fit=crop',
      caption: 'Waiting area',
      is_primary: false,
      created_at: new Date().toISOString(),
    },
  ]

  // Generate chargers
  const chargers: Charger[] = [
    {
      id: `CHR-${Math.random().toString(36).substring(2, 11)}`,
      station_id: id,
      charger_type: 'CCS',
      connector_count: 2,
      availability_status: Math.random() > 0.3 ? 'available' : 'in_use',
      power_kw: 50,
      is_active: true,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    },
    {
      id: `CHR-${Math.random().toString(36).substring(2, 11)}`,
      station_id: id,
      charger_type: 'CCS',
      connector_count: 3,
      availability_status: Math.random() > 0.3 ? 'available' : 'in_use',
      power_kw: 75,
      is_active: true,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    },
    {
      id: `CHR-${Math.random().toString(36).substring(2, 11)}`,
      station_id: id,
      charger_type: 'CHAdeMO',
      connector_count: 2,
      availability_status: 'available',
      power_kw: 50,
      is_active: true,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    },
    {
      id: `CHR-${Math.random().toString(36).substring(2, 11)}`,
      station_id: id,
      charger_type: 'AC',
      connector_count: 4,
      availability_status: 'in_use',
      power_kw: 7,
      is_active: true,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    },
    {
      id: `CHR-${Math.random().toString(36).substring(2, 11)}`,
      station_id: id,
      charger_type: 'AC',
      connector_count: 2,
      availability_status: 'maintenance',
      power_kw: 7,
      is_active: false,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    },
  ]

  return {
    id,
    name: `Station ${id}`,
    address: '123 Main Street, Tunis, Tunisia',
    geometry: {
      type: 'Point',
      coordinates: [10.1815 + Math.random() * 0.1, 36.8065 + Math.random() * 0.1],
    },
    amenities: ['WiFi', 'Parking', 'Cafe', 'Restrooms'],
    operating_hours: '24/7',
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
    chargers,
    images,
  }
}

export interface OperatingHours {
  weekday: string
  open: string
  close: string
}

export function parseOperatingHours(hours: string): OperatingHours[] {
  // Parse operating hours string
  // Example: "Mon-Fri: 8AM - 10PM, Sat-Sun: 9AM - 9PM"

  const days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

  return days.map((day) => {
    // Simple parsing - in real implementation, use proper parsing
    return {
      weekday: day,
      open: '8:00 AM',
      close: '10:00 PM',
    }
  })
}

export function calculateDistance(
  lat1: number,
  lng1: number,
  lat2: number,
  lng2: number,
): number {
  const R = 6371 // Earth's radius in kilometers
  const dLat = (lat2 - lat1) * (Math.PI / 180)
  const dLng = (lng2 - lng1) * (Math.PI / 180)
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(lat1 * (Math.PI / 180)) *
      Math.cos(lat2 * (Math.PI / 180)) *
      Math.sin(dLng / 2) * Math.sin(dLng / 2)
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
  return R * c
}

export function getStationDistance(
  station: Station,
  userLat: number,
  userLng: number,
): number {
  if (!station.geometry || station.geometry.type !== 'Point') {
    return 0
  }

  const [lng, lat] = station.geometry.coordinates
  return calculateDistance(userLat, userLng, lat, lng)
}

export function formatDistance(distance: number): string {
  if (distance < 1) {
    return `${(distance * 1000).toFixed(0)}m`
  }
  return `${distance.toFixed(1)}km`
}
