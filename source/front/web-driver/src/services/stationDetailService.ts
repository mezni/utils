export interface Station {
  id: string
  name: string
  address: string
  amenities: string[]
  geometry: {
    type: 'Point'
    coordinates: [number, number]
  }
  operating_hours?: string
  images?: Array<{ url: string }>
  chargers?: Array<{
    charger_type: string
    power_kw: number
    connector_count: number
    availability_status: string
  }>
}

export async function fetchStationDetail(id: string): Promise<Station> {
  const response = await fetch(`/api/stations/${id}`)
  if (!response.ok) throw new Error('Failed to fetch station detail')
  return response.json()
}