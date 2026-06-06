import { useTranslation } from 'react-i18next'

export const UsersScreen = () => {
  const { t } = useTranslation()
  return (
    <div className="p-6">
      <h1 className="text-2xl font-bold text-text-primary mb-6">{t('dashboard.users')}</h1>
      <div className="bg-surface-panel rounded-lg p-6 text-text-muted">
        User management coming soon
      </div>
    </div>
  )
}