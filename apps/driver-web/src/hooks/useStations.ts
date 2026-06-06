import { useMemo } from 'react'
import { stations } from '../mocks/stations'
import { chargers } from '../mocks/chargers'
import { reviews } from '../mocks/reviews'
import type { Charger, Review } from '../types'

export function useStations() {
  return useMemo(() => ({
    stations,

    getStationById(id: string) {
      return stations.find(s => s.id === id)
    },

    getChargersForStation(stationId: string): Charger[] {
      return chargers.filter(c => c.stationId === stationId)
    },

    getReviewsForStation(stationId: string): Review[] {
      return reviews.filter(r => r.stationId === stationId)
    },
  }), [])
}
