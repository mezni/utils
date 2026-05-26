import React from "react"

interface SelectSettingProps {
  label: string
  options: { value: string; label: string }[]
  value: string
  onChange: (value: string) => void
}

export function SelectSetting({ label, options, value, onChange }: SelectSettingProps) {
  return (
    <div>
      <label className="mb-1 block text-sm font-medium text-gray-700">{label}</label>
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="block w-full rounded-md border border-gray-300 bg-white px-3 py-2 text-sm text-gray-900 shadow-sm focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent"
      >
        {options.map((opt) => (
          <option key={opt.value} value={opt.value}>
            {opt.label}
          </option>
        ))}
      </select>
    </div>
  )
}
