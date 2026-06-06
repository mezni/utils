import { useTranslation } from 'react-i18next'

export const StationEditScreen = () => {
  const { t } = useTranslation()

  return (
    <div className="p-6">
      <h1 className="text-2xl font-bold text-text-primary mb-6">{t('dashboard.stationEdit')}</h1>
      <div className="max-w-2xl bg-surface-panel rounded-lg p-6">
        <form className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-text-primary mb-2">{t('form.stationName')}</label>
            <input type="text" className="w-full px-4 py-2 border border-border-default rounded-lg" defaultValue="Centre Urbain Nord" />
          </div>
          <div>
            <label className="block text-sm font-medium text-text-primary mb-2">{t('form.address')}</label>
            <input type="text" className="w-full px-4 py-2 border border-border-default rounded-lg" defaultValue="Avenue Habib Bourguiba, Tunis" />
          </div>
          <div className="flex gap-4">
            <button type="button" className="px-6 py-2 bg-brand-primary text-white rounded-lg hover:bg-brand-dark transition-colors">
              {t('form.save')}
            </button>
            <button type="button" className="px-6 py-2 border border-border-default rounded-lg hover:bg-surface-hover transition-colors">
              {t('form.cancel')}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}