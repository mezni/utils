import React from "react"

interface MetricChipProps {
  label: string
  value: number | null
  isLoading?: boolean
}

export function MetricChip({ label, value, isLoading }: MetricChipProps) {
  return (
    <div className="rounded-2xl border border-gray-200 bg-white p-6 shadow-card">
      <p className="text-sm font-medium text-gray-500">{label}</p>
      {isLoading ? (
        <div className="mt-2 h-8 w-20 animate-pulse rounded-md bg-gray-200" />
      ) : (
        <p className="mt-2 text-3xl font-bold text-gray-900">
          {value !== null ? value : 0}
        </p>
      )}
    </div>
  )
}
