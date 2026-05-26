export interface Station {
  id: string
  name: string
  address: string
  city: string
  latitude: number
  longitude: number
  available_chargers: number
  distance_meters: number
  is_operational: boolean
  is_test: boolean
}

export interface Charger {
  id: string
  station_id: string
  connector_type_id: string
  power_kw: number
  current_type: string
  status: string
}
