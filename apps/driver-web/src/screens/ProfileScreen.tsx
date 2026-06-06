import { useTranslation } from 'react-i18next'
import MobileTopBar from '../components/MobileTopBar'
import { useState } from 'react'

export default function ProfileScreen() {
  const { t } = useTranslation()
  const [sidebarOpen, setSidebarOpen] = useState(false)

  return (
    <div className="flex h-screen flex-col">
      <MobileTopBar sidebarOpen={sidebarOpen} onToggleSidebar={() => setSidebarOpen(prev => !prev)} />
      <div className="flex-1 overflow-y-auto">
        <div className="mx-auto max-w-md px-4 py-8">
          <div className="mb-6 flex flex-col items-center">
            <div className="flex h-20 w-20 items-center justify-center rounded-full bg-brand-primary text-3xl font-bold text-white">
              A
            </div>
            <h2 className="mt-3 text-lg font-semibold text-neutral-700">{t('profile.title')}</h2>
          </div>
          <div className="space-y-4">
            <div>
              <label className="mb-1 block text-xs font-medium text-neutral-500">{t('profile.name')}</label>
              <input
                type="text"
                defaultValue="Ahmed Ben Salem"
                className="w-full rounded-lg border border-neutral-200 px-3 py-2 text-sm focus:border-brand-primary focus:outline-none focus:ring-1 focus:ring-brand-primary"
              />
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-neutral-500">{t('profile.email')}</label>
              <input
                type="email"
                defaultValue="ahmed.bensalem@example.tn"
                className="w-full rounded-lg border border-neutral-200 px-3 py-2 text-sm focus:border-brand-primary focus:outline-none focus:ring-1 focus:ring-brand-primary"
              />
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-neutral-500">{t('profile.phone')}</label>
              <input
                type="tel"
                defaultValue="+216 52 123 456"
                className="w-full rounded-lg border border-neutral-200 px-3 py-2 text-sm focus:border-brand-primary focus:outline-none focus:ring-1 focus:ring-brand-primary"
              />
            </div>
            <button className="w-full rounded-lg bg-brand-primary py-2.5 text-sm font-medium text-white">
              {t('profile.save')}
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}
