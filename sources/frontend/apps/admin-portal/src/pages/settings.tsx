export function SettingsPage() {
  return (
    <div>
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-gray-900">Settings</h1>
        <p className="mt-1 text-sm text-gray-500">Platform configuration</p>
      </div>
      <div className="grid gap-6 md:grid-cols-2">
        <div className="rounded-2xl border border-gray-200 bg-surface-card p-6 shadow-card">
          <h3 className="text-sm font-medium text-gray-900">Infrastructure Types</h3>
          <p className="mt-1 text-xs text-gray-500">Manage connector type definitions.</p>
          <p className="mt-4 text-sm text-gray-500">Coming in a future update.</p>
        </div>
        <div className="rounded-2xl border border-gray-200 bg-surface-card p-6 shadow-card">
          <h3 className="text-sm font-medium text-gray-900">App Settings</h3>
          <p className="mt-1 text-xs text-gray-500">Branding, maps, and general preferences.</p>
          <p className="mt-4 text-sm text-gray-500">Coming in a future update.</p>
        </div>
      </div>
    </div>
  )
}
