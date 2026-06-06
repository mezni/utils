import { useState, useMemo, useCallback } from 'react'
import { stations } from '../mocks/stations'
import type { FilterState, Station } from '../types'

export function useMockFilter() {
  const [filter, setFilter] = useState<FilterState>({
    chargerType: 'all',
    availability: 'all',
    searchQuery: '',
  })

  const setChargerType = useCallback((type: FilterState['chargerType']) =>
    setFilter(prev => ({ ...prev, chargerType: type })), [])

  const setAvailability = useCallback((avail: FilterState['availability']) =>
    setFilter(prev => ({ ...prev, availability: avail })), [])

  const setSearchQuery = useCallback((query: string) =>
    setFilter(prev => ({ ...prev, searchQuery: query })), [])

  const filteredStations: Station[] = useMemo(() => {
    return stations.filter(s => {
      if (filter.availability === 'available' && s.availability !== 'available') return false
      if (filter.searchQuery) {
        const q = filter.searchQuery.toLowerCase()
        if (!s.name.toLowerCase().includes(q) && !s.address.toLowerCase().includes(q)) return false
      }
      return true
    })
  }, [filter])

  return { filter, setChargerType, setAvailability, setSearchQuery, filteredStations }
}
