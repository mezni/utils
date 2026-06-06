import { useState, useCallback, useRef } from 'react'
import type { Station, Charger, Review } from '../types'

export interface UsePaginatedState<T> {
  items: T[]
  loading: boolean
  error: Error | null
  currentPage: number
  pageSize: number
  total: number
  hasNextPage: boolean
}

export interface UsePaginatedReturn<T> extends UsePaginatedState<T> {
  nextPage: () => void
  prevPage: () => void
  goToPage: (page: number) => void
  retry: () => Promise<void>
}

export function usePaginated<T>(
  fetchFunction: (page: number, pageSize: number) => Promise<T[]>,
  pageSize = 10,
): UsePaginatedReturn<T> {
  const [state, setState] = useState<UsePaginatedState<T>>({
    items: [],
    loading: true,
    error: null,
    currentPage: 1,
    pageSize,
    total: 0,
    hasNextPage: false,
  })

  const isMounted = useRef(true)

  const fetchPage = useCallback(
    async (page: number) => {
      if (!isMounted.current) return

      setState(prev => ({ ...prev, loading: true, error: null }))

      try {
        const items = await fetchFunction(page, pageSize)
        if (!isMounted.current) return

        setState(prev => ({
          ...prev,
          items,
          currentPage: page,
          loading: false,
          hasNextPage: items.length === pageSize,
        }))
      } catch (error) {
        if (!isMounted.current) return

        setState(prev => ({
          ...prev,
          loading: false,
          error: error instanceof Error ? error : new Error('Unknown error'),
        }))
      }
    },
    [fetchFunction, pageSize],
  )

  const nextPage = useCallback(() => {
    if (state.hasNextPage) {
      fetchPage(state.currentPage + 1)
    }
  }, [fetchPage, state.hasNextPage, state.currentPage])

  const prevPage = useCallback(() => {
    if (state.currentPage > 1) {
      fetchPage(state.currentPage - 1)
    }
  }, [fetchPage, state.currentPage])

  const goToPage = useCallback(
    (page: number) => {
      if (page >= 1) {
        fetchPage(page)
      }
    },
    [fetchPage],
  )

  const retry = useCallback(() => fetchPage(state.currentPage), [fetchPage, state.currentPage])

  // Initial fetch
  if (state.loading && state.items.length === 0) {
    fetchPage(1)
  }

  // Cleanup on unmount
  if (isMounted) {
    return { ...state, nextPage, prevPage, goToPage, retry }
  }

  return { ...state, nextPage, prevPage, goToPage, retry }
}
