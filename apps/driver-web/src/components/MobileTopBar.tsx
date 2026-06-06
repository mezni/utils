import { useTranslation } from 'react-i18next'

interface MobileTopBarProps {
  brandName?: string
  sidebarOpen: boolean
  onToggleSidebar: () => void
  notificationCount?: number
  onNotificationClick?: () => void
}

export default function MobileTopBar({
  brandName,
  sidebarOpen,
  onToggleSidebar,
  notificationCount = 0,
  onNotificationClick,
}: MobileTopBarProps) {
  const { t } = useTranslation()

  return (
    <header className="flex items-center justify-between bg-brand-primary px-4 py-3 text-white shadow-md">
      <button
        onClick={onToggleSidebar}
        className="rounded p-1 focus:outline-none focus:ring-2 focus:ring-white"
        aria-label={sidebarOpen ? 'Close menu' : 'Open menu'}
      >
        <svg className="h-6 w-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          {sidebarOpen ? (
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
          ) : (
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h16" />
          )}
        </svg>
      </button>
      <span className="text-lg font-bold">{brandName || t('app.name')}</span>
      <button
        onClick={onNotificationClick}
        className="relative rounded p-1 focus:outline-none focus:ring-2 focus:ring-white"
        aria-label="Notifications"
      >
        <svg className="h-6 w-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 17h5l-1.405-1.405A2.032 2.032 0 0118 14.158V11a6.002 6.002 0 00-4-5.659V5a2 2 0 10-4 0v.341C7.67 6.165 6 8.388 6 11v3.159c0 .538-.214 1.055-.595 1.436L4 17h5m6 0v1a3 3 0 11-6 0v-1m6 0H9" />
        </svg>
        {notificationCount > 0 && (
          <span className="absolute -right-1 -top-1 flex h-5 w-5 items-center justify-center rounded-full bg-semantic-error text-xs font-bold text-white">
            {notificationCount}
          </span>
        )}
      </button>
    </header>
  )
}
