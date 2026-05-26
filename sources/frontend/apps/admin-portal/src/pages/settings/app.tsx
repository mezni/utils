import { SettingsCard } from "@bornemap/ui"

export function AppSettingsPage() {
  return (
    <div className="grid gap-6 md:grid-cols-3">
      <SettingsCard title="Branding" description="Logo, colors, and favicon configuration">
        <p className="text-sm text-gray-400">Coming in a future release.</p>
      </SettingsCard>
      <SettingsCard title="Map Tokens" description="Map provider API keys and tile configuration">
        <p className="text-sm text-gray-400">Coming in a future release.</p>
      </SettingsCard>
      <SettingsCard title="Dropzones" description="File upload targets and storage configuration">
        <p className="text-sm text-gray-400">Coming in a future release.</p>
      </SettingsCard>
    </div>
  )
}
