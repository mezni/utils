import { describe, it, expect } from 'vitest'
import { ApiError, NetworkError, ValidationError, getErrorMessage, isRetryableError, getRetryDelay } from '../utils/errors'

describe('Error Classes', () => {
  it('creates ApiError with status', () => {
    const error = new ApiError(404, undefined, 'Not found')
    expect(error.status).toBe(404)
    expect(error.message).toBe('Not found')
    expect(error.name).toBe('ApiError')
  })

  it('creates NetworkError', () => {
    const error = new NetworkError('Connection failed')
    expect(error.message).toBe('Connection failed')
    expect(error.name).toBe('NetworkError')
  })

  it('creates ValidationError with field', () => {
    const error = new ValidationError('email', 'Invalid email')
    expect(error.field).toBe('email')
    expect(error.name).toBe('ValidationError')
  })
})

describe('getErrorMessage', () => {
  it('returns friendly message for 400 error', () => {
    const error = new ApiError(400)
    expect(getErrorMessage(error)).toContain('Bad request')
  })

  it('returns friendly message for 401 error', () => {
    const error = new ApiError(401)
    expect(getErrorMessage(error)).toContain('Unauthorized')
  })

  it('returns friendly message for 404 error', () => {
    const error = new ApiError(404)
    expect(getErrorMessage(error)).toContain('Not found')
  })

  it('returns friendly message for 500 error', () => {
    const error = new ApiError(500)
    expect(getErrorMessage(error)).toContain('Server error')
  })

  it('returns message for NetworkError', () => {
    const error = new NetworkError()
    expect(getErrorMessage(error)).toContain('Network error')
  })

  it('returns message for ValidationError', () => {
    const error = new ValidationError('username')
    expect(getErrorMessage(error)).toContain('Invalid input')
  })

  it('returns custom error message', () => {
    const error = new Error('Custom error')
    expect(getErrorMessage(error)).toBe('Custom error')
  })
})

describe('isRetryableError', () => {
  it('returns true for NetworkError', () => {
    const error = new NetworkError()
    expect(isRetryableError(error)).toBe(true)
  })

  it('returns true for 500+ status', () => {
    const error = new ApiError(500)
    expect(isRetryableError(error)).toBe(true)
  })

  it('returns true for 408 status (timeout)', () => {
    const error = new ApiError(408)
    expect(isRetryableError(error)).toBe(true)
  })

  it('returns true for 429 status (rate limit)', () => {
    const error = new ApiError(429)
    expect(isRetryableError(error)).toBe(true)
  })

  it('returns false for 400 status', () => {
    const error = new ApiError(400)
    expect(isRetryableError(error)).toBe(false)
  })

  it('returns false for generic error', () => {
    const error = new Error('Generic error')
    expect(isRetryableError(error)).toBe(false)
  })
})

describe('getRetryDelay', () => {
  it('returns exponential backoff', () => {
    const delay0 = getRetryDelay(0)
    const delay1 = getRetryDelay(1)
    const delay2 = getRetryDelay(2)

    expect(delay1).toBeGreaterThan(delay0)
    expect(delay2).toBeGreaterThan(delay1)
  })

  it('caps maximum delay at 32s', () => {
    const delay = getRetryDelay(10)
    expect(delay).toBeLessThanOrEqual(32000 * 1.1) // Allow 10% jitter
  })

  it('supports custom base delay', () => {
    const delay = getRetryDelay(0, 500)
    expect(delay).toBeLessThanOrEqual(500 * 1.1)
    expect(delay).toBeGreaterThanOrEqual(500 * 0.9)
  })
})
