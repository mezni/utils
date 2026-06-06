import { useRole } from '../../hooks/useRole'

export const TopBar = () => {
  const { role } = useRole()

  return (
    <div className="h-16 border-b border-border-default bg-surface-panel flex items-center justify-between px-6">
      <div className="flex items-center gap-4">
        <h2 className="text-lg font-semibold text-text-primary">
          {role === 'partner' ? 'Partner Dashboard' : 'Admin Dashboard'}
        </h2>
      </div>
      <div className="flex items-center gap-4">
        <button className="text-text-muted hover:text-text-primary">🔔</button>
        <div className="w-8 h-8 rounded-full bg-brand-primary text-white flex items-center justify-center text-sm font-medium">
          P
        </div>
      </div>
    </div>
  )
}