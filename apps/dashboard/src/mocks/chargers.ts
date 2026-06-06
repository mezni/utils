import { Charger } from '../types'

export const mockChargers: Charger[] = [
  {
    id: 'CHG-1',
    stationId: 'STN-1',
    connectorType: 'Type2',
    powerRating: 22,
    status: 'available'
  },
  {
    id: 'CHG-2',
    stationId: 'STN-1',
    connectorType: 'CCS',
    powerRating: 50,
    status: 'available'
  },
  {
    id: 'CHG-3',
    stationId: 'STN-1',
    connectorType: 'CHAdeMO',
    powerRating: 50,
    status: 'available'
  },
  {
    id: 'CHG-4',
    stationId: 'STN-2',
    connectorType: 'CCS',
    powerRating: 100,
    status: 'available'
  }
]

export const getChargersByStation = (stationId: string): Charger[] => {
  return mockChargers.filter(c => c.stationId === stationId)
}

export const getChargerById = (id: string): Charger | undefined => {
  return mockChargers.find(c => c.id === id)
}