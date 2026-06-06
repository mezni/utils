import { renderHook, act } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import { useAsync } from '../useAsync'

describe('useAsync Hook', () => {
  it('initializes with loading state', () => {
    const mockFn = vi.fn(() => Promise.resolve('data'))
    const { result } = renderHook(() => useAsync(mockFn, false))

    expect(result.current.loading).toBe(false)
    expect(result.current.data).toBe(null)
    expect(result.current.error).toBe(null)
  })

  it('handles successful async function', async () => {
    const mockData = { id: '1', name: 'Test' }
    const mockFn = vi.fn(() => Promise.resolve(mockData))

    const { result } = renderHook(() => useAsync(mockFn, true))

    // Wait for async execution
    await act(async () => {
      await new Promise(resolve => setTimeout(resolve, 100))
    })

    expect(result.current.loading).toBe(false)
    expect(result.current.data).toEqual(mockData)
    expect(result.current.error).toBe(null)
  })

  it('handles async function errors', async () => {
    const testError = new Error('Test error')
    const mockFn = vi.fn(() => Promise.reject(testError))

    const { result } = renderHook(() => useAsync(mockFn, true))

    await act(async () => {
      await new Promise(resolve => setTimeout(resolve, 100))
    })

    expect(result.current.loading).toBe(false)
    expect(result.current.data).toBe(null)
    expect(result.current.error).toEqual(testError)
  })

  it('provides retry function', async () => {
    let callCount = 0
    const mockFn = vi.fn(() => {
      callCount++
      return Promise.resolve(`call-${callCount}`)
    })

    const { result } = renderHook(() => useAsync(mockFn, false))

    await act(async () => {
      result.current.retry()
      await new Promise(resolve => setTimeout(resolve, 100))
    })

    expect(result.current.data).toBe('call-1')

    await act(async () => {
      result.current.retry()
      await new Promise(resolve => setTimeout(resolve, 100))
    })

    expect(result.current.data).toBe('call-2')
  })

  it('provides reset function', async () => {
    const mockFn = vi.fn(() => Promise.resolve('data'))
    const { result } = renderHook(() => useAsync(mockFn, true))

    await act(async () => {
      await new Promise(resolve => setTimeout(resolve, 100))
    })

    expect(result.current.data).toBe('data')

    act(() => {
      result.current.reset()
    })

    expect(result.current.data).toBe(null)
    expect(result.current.loading).toBe(false)
    expect(result.current.error).toBe(null)
  })
})
