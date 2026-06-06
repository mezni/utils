import React from 'react'
import { getErrorMessage } from '../utils/errors'

interface ErrorStateProps {
  error: Error
  onRetry?: () => void
  title?: string
}

/**
 * Component for displaying error states
 */
export function ErrorStateComponent({
  error,
  onRetry,
  title = 'Error',
}: ErrorStateProps) {
  const message = getErrorMessage(error)

  return (
    <div className="flex min-h-96 flex-col items-center justify-center rounded-lg bg-neutral-50 p-8 text-center">
      <div className="mb-4 text-5xl">⚠️</div>
      <h2 className="mb-2 text-lg font-semibold text-neutral-900">{title}</h2>
      <p className="mb-6 text-sm text-neutral-600">{message}</p>
      {onRetry && (
        <button
          onClick={onRetry}
          className="rounded-lg bg-brand-primary px-4 py-2 text-sm text-white hover:bg-brand-dark"
        >
          Try again
        </button>
      )}
    </div>
  )
}
