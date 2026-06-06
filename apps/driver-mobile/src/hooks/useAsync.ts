import { useState, useCallback } from 'react'

export interface UseAsyncState<T> {
  data: T | null
  loading: boolean
  error: Error | null
}

export interface UseAsyncReturn<T> extends UseAsyncState<T> {
  retry: () => Promise<void>
  reset: () => void
}

export function useAsync<T>(
  asyncFunction: () => Promise<T>,
  immediate = true,
): UseAsyncReturn<T> {
  const [state, setState] = useState<UseAsyncState<T>>({
    data: null,
    loading: immediate,
    error: null,
  })

  const execute = useCallback(async () => {
    setState({ data: null, loading: true, error: null })
    try {
      const response = await asyncFunction()
      setState({ data: response, loading: false, error: null })
    } catch (error) {
      setState({
        data: null,
        loading: false,
        error: error instanceof Error ? error : new Error('Unknown error'),
      })
    }
  }, [asyncFunction])

  const retry = useCallback(execute, [execute])
  const reset = useCallback(
    () => setState({ data: null, loading: false, error: null }),
    [],
  )

  // Execute on mount if immediate
  if (immediate && state.loading && state.data === null && state.error === null) {
    execute()
  }

  return { ...state, retry, reset }
}
