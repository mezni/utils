export interface Station {
  id: string
  name: string
  status: 'active' | 'maintenance'
  latitude: number
  longitude: number
  location: {
    type: 'Point'
    coordinates: [number, number]
  }
  distance?: number
}
