import { useTranslation } from 'react-i18next'

export const PartnersScreen = () => {
  const { t } = useTranslation()
  return (
    <div className="p-6">
      <h1 className="text-2xl font-bold text-text-primary mb-6">{t('dashboard.partners')}</h1>
      <div className="bg-surface-panel rounded-lg p-6 text-text-muted">
        Partner management coming soon
      </div>
    </div>
  )
}