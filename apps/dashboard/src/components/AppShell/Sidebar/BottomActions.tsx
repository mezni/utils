import { useRole } from '../../../hooks/useRole.ts'
import { useTranslation } from 'react-i18next'

export const BottomActions = () => {
  const { toggleRole, role } = useRole()
  const { t } = useTranslation()

  return (
    <div className="p-4 border-t border-border-default space-y-2">
      <button
        onClick={toggleRole}
        className="w-full px-4 py-2 bg-brand-sageLight text-brand-primary rounded-lg hover:bg-brand-sageDark transition-colors text-sm"
      >
        {t('role.switch')}: {role === 'partner' ? t('role.admin') : t('role.partner')}
      </button>
      <button className="w-full px-4 py-2 text-text-muted hover:text-text-primary text-sm">
        Déconnexion
      </button>
    </div>
  )
}