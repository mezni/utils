import React from 'react'

/**
 * Component for displaying loading skeleton
 */
export function LoadingSkeleton() {
  return (
    <div className="space-y-4">
      {[...Array(3)].map((_, i) => (
        <div key={i} className="animate-pulse rounded-lg bg-neutral-200 p-4">
          <div className="mb-3 h-4 w-1/4 rounded bg-neutral-300"></div>
          <div className="mb-2 h-3 w-full rounded bg-neutral-300"></div>
          <div className="h-3 w-3/4 rounded bg-neutral-300"></div>
        </div>
      ))}
    </div>
  )
}

/**
 * Component for displaying loading state in a card
 */
export function LoadingCard() {
  return (
    <div className="animate-pulse rounded-lg bg-white p-4 shadow">
      <div className="mb-4 h-6 w-1/3 rounded bg-neutral-200"></div>
      <div className="space-y-2">
        <div className="h-4 w-full rounded bg-neutral-200"></div>
        <div className="h-4 w-4/5 rounded bg-neutral-200"></div>
      </div>
    </div>
  )
}
