import { useTranslation } from 'react-i18next'
import { NavigationItem } from './NavigationItem'
import { BrandHeader } from './BrandHeader'
import { BottomActions } from './BottomActions'
import { useRole } from '../../../hooks/useRole.ts'

export const Sidebar = () => {
  const { t } = useTranslation()
  const { role } = useRole()

  const partnerNav = [
    { path: '/', icon: '🏠', label: t('dashboard.overview') },
    { path: '/stations', icon: '⚡', label: t('dashboard.myStations') },
    { path: '/chargers', icon: '🔌', label: t('dashboard.chargerManagement') },
    { path: '/availability', icon: '📊', label: t('dashboard.availabilityUpdate') },
    { path: '/reports', icon: '📈', label: t('dashboard.reports') }
  ]

  const adminNav = [
    { path: '/', icon: '🏠', label: t('dashboard.overview') },
    { path: '/users', icon: '👤', label: t('dashboard.users') },
    { path: '/partners', icon: '🏢', label: t('dashboard.partners') },
    { path: '/admin/stations', icon: '⚡', label: t('dashboard.stations') },
    { path: '/admin/chargers', icon: '🔌', label: t('dashboard.chargers') },
    { path: '/admin/reviews', icon: '⭐', label: t('dashboard.reviews') },
    { path: '/reports', icon: '📈', label: t('dashboard.reports') }
  ]

  const navItems = role === 'partner' ? partnerNav : adminNav

  return (
    <div className="w-64 h-full bg-surface-panel border-r border-border-default flex flex-col">
      <BrandHeader />
      <nav className="flex-1 overflow-y-auto p-2">
        {navItems.map((item) => (
          <NavigationItem key={item.path} path={item.path} icon={item.icon} label={item.label} />
        ))}
      </nav>
      <BottomActions />
    </div>
  )
}