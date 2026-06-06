/**
 * Custom error class for API errors
 */
export class ApiError extends Error {
  constructor(
    public status: number,
    public originalError?: Error,
    message = `API Error: ${status}`,
  ) {
    super(message)
    this.name = 'ApiError'
  }
}

/**
 * Custom error class for network errors
 */
export class NetworkError extends Error {
  constructor(message = 'Network error', public originalError?: Error) {
    super(message)
    this.name = 'NetworkError'
  }
}

/**
 * Custom error class for validation errors
 */
export class ValidationError extends Error {
  constructor(
    public field: string,
    message = `Validation error: ${field}`,
  ) {
    super(message)
    this.name = 'ValidationError'
  }
}

/**
 * Get user-friendly error message
 */
export function getErrorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    switch (error.status) {
      case 400:
        return 'Bad request. Please check your input.'
      case 401:
        return 'Unauthorized. Please log in again.'
      case 403:
        return 'Forbidden. You do not have permission.'
      case 404:
        return 'Not found. The requested resource does not exist.'
      case 500:
        return 'Server error. Please try again later.'
      default:
        return `API Error: ${error.status}`
    }
  }

  if (error instanceof NetworkError) {
    return 'Network error. Please check your internet connection.'
  }

  if (error instanceof ValidationError) {
    return `Invalid input: ${error.field}`
  }

  if (error instanceof Error) {
    return error.message
  }

  return 'An unknown error occurred'
}

/**
 * Determine if error is retryable
 */
export function isRetryableError(error: unknown): boolean {
  if (error instanceof NetworkError) {
    return true
  }

  if (error instanceof ApiError) {
    // Retry on server errors (5xx) and timeout-like errors
    return error.status >= 500 || error.status === 408 || error.status === 429
  }

  return false
}

/**
 * Create exponential backoff retry strategy
 */
export function getRetryDelay(attempt: number, baseDelay = 1000): number {
  // Exponential backoff: 1s, 2s, 4s, 8s, etc (max 32s)
  const delay = Math.min(baseDelay * Math.pow(2, attempt), 32000)
  // Add random jitter (±10%)
  const jitter = delay * 0.1 * (Math.random() * 2 - 1)
  return Math.round(delay + jitter)
}
