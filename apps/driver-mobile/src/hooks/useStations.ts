import { useState, useCallback } from 'react'
import { stations } from '../mocks/stations'
import type { Station } from '../types'

export function useStations() {
  const [allStations] = useState<Station[]>(stations)

  const getStationById = useCallback(
    (id: string) => allStations.find((s) => s.id === id) ?? null,
    [allStations],
  )

  return { allStations, getStationById }
}
