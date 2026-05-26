import React from "react"

interface SettingsCardProps {
  title: string
  description?: string
  children: React.ReactNode
}

export function SettingsCard({ title, description, children }: SettingsCardProps) {
  return (
    <div className="rounded-2xl border border-gray-200 bg-white p-6 shadow-card">
      <h3 className="text-sm font-semibold text-gray-900">{title}</h3>
      {description && (
        <p className="mt-1 text-xs text-gray-500">{description}</p>
      )}
      <div className="mt-4">{children}</div>
    </div>
  )
}
