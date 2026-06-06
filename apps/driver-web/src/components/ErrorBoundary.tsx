import React, { useState, useCallback } from 'react'

interface ErrorBoundaryProps {
  children: React.ReactNode
  onError?: (error: Error, errorInfo: React.ErrorInfo) => void
  fallback?: (error: Error, retry: () => void) => React.ReactNode
}

interface ErrorBoundaryState {
  hasError: boolean
  error: Error | null
}

/**
 * Error boundary component for catching React errors
 * Provides error UI and recovery mechanism
 */
export class ErrorBoundary extends React.Component<
  ErrorBoundaryProps,
  ErrorBoundaryState
> {
  constructor(props: ErrorBoundaryProps) {
    super(props)
    this.state = { hasError: false, error: null }
  }

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { hasError: true, error }
  }

  componentDidCatch(error: Error, errorInfo: React.ErrorInfo) {
    // Log to console in development
    if (process.env.NODE_ENV === 'development') {
      console.error('Error caught by ErrorBoundary:', error, errorInfo)
    }

    // Call custom error handler if provided
    this.props.onError?.(error, errorInfo)
  }

  handleReset = () => {
    this.setState({ hasError: false, error: null })
  }

  render() {
    if (this.state.hasError && this.state.error) {
      // Use custom fallback if provided
      if (this.props.fallback) {
        return this.props.fallback(this.state.error, this.handleReset)
      }

      // Default error UI
      return (
        <div className="flex min-h-screen flex-col items-center justify-center bg-neutral-50 px-4">
          <div className="rounded-lg bg-white p-6 shadow-lg">
            <h1 className="mb-2 text-xl font-bold text-neutral-900">
              Something went wrong
            </h1>
            <p className="mb-4 text-sm text-neutral-600">
              {this.state.error.message}
            </p>
            <button
              onClick={this.handleReset}
              className="rounded-lg bg-brand-primary px-4 py-2 text-white hover:bg-brand-dark"
            >
              Try again
            </button>
          </div>
        </div>
      )
    }

    return this.props.children
  }
}
