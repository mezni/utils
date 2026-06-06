import { useState, useMemo, useCallback, useEffect, useRef } from 'react'
import type { Station, FilterState } from '../types'

interface UseMockFilterReturn {
  filteredStations: Station[]
  searchQuery: string
  setSearchQuery: (query: string) => void
  filterState: FilterState
  setFilterState: (state: Partial<FilterState>) => void
  clearFilters: () => void
}

export function useMockFilter(stations: Station[]): UseMockFilterReturn {
  const [searchQuery, setSearchQueryRaw] = useState('')
  const [filterState, setFilterStateRaw] = useState<FilterState>({
    chargerType: 'all',
    availability: 'all',
    searchQuery: '',
  })
  const debounceTimer = useRef<ReturnType<typeof setTimeout> | null>(null)
  const [debouncedQuery, setDebouncedQuery] = useState('')

  const setSearchQuery = useCallback((query: string) => {
    setSearchQueryRaw(query)
    if (debounceTimer.current) {
      clearTimeout(debounceTimer.current)
    }
    debounceTimer.current = setTimeout(() => {
      setDebouncedQuery(query)
    }, 300)
  }, [])

  const setFilterState = useCallback((state: Partial<FilterState>) => {
    setFilterStateRaw((prev) => ({ ...prev, ...state }))
  }, [])

  const clearFilters = useCallback(() => {
    setSearchQueryRaw('')
    setDebouncedQuery('')
    setFilterStateRaw({ chargerType: 'all', availability: 'all', searchQuery: '' })
  }, [])

  useEffect(() => {
    return () => {
      if (debounceTimer.current) {
        clearTimeout(debounceTimer.current)
      }
    }
  }, [])

  const filteredStations = useMemo(() => {
    let result = stations

    if (debouncedQuery) {
      const q = debouncedQuery.toLowerCase()
      result = result.filter(
        (s) =>
          s.name.toLowerCase().includes(q) ||
          s.address.toLowerCase().includes(q),
      )
    }

    if (filterState.chargerType !== 'all') {
      result = result.filter((s) => s.availability === 'available')
    }

    if (filterState.availability === 'available') {
      result = result.filter((s) => s.availableCount > 0)
    }

    return result
  }, [stations, debouncedQuery, filterState])

  return {
    filteredStations,
    searchQuery,
    setSearchQuery,
    filterState,
    setFilterState,
    clearFilters,
  }
}
