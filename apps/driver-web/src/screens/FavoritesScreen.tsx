import { useTranslation } from 'react-i18next'
import { useNavigate } from 'react-router-dom'
import MobileTopBar from '../components/MobileTopBar'
import StationCard from '../components/StationCard'
import { useFavorites } from '../hooks/useFavorites'
import { useStations } from '../hooks/useStations'
import { useState } from 'react'

export default function FavoritesScreen() {
  const { t } = useTranslation()
  const navigate = useNavigate()
  const { favorites } = useFavorites()
  const { stations } = useStations()
  const [sidebarOpen, setSidebarOpen] = useState(false)

  const favoriteStations = stations.filter(s => favorites.includes(s.id))

  return (
    <div className="flex h-screen flex-col">
      <MobileTopBar sidebarOpen={sidebarOpen} onToggleSidebar={() => setSidebarOpen(prev => !prev)} />
      <div className="flex-1 overflow-y-auto">
        <div className="mx-auto max-w-2xl px-4 py-4">
          <h2 className="mb-4 text-base font-semibold text-neutral-700">{t('favorites.title')}</h2>
          {favoriteStations.length === 0 ? (
            <div className="mt-12 text-center">
              <p className="text-sm text-neutral-400">{t('favorites.empty')}</p>
            </div>
          ) : (
            <div className="space-y-3">
              {favoriteStations.map(s => (
                <StationCard key={s.id} station={s} onClick={(id) => navigate(`/stations/${id}`)} />
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
