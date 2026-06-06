import { useParams } from 'react-router-dom'
import { useTranslation } from 'react-i18next'
import MobileTopBar from '../components/MobileTopBar'
import ChargerRow from '../components/ChargerRow'
import ReviewCard from '../components/ReviewCard'
import { useStations } from '../hooks/useStations'
import { useState } from 'react'

export default function StationDetailScreen() {
  const { id } = useParams<{ id: string }>()
  const { t } = useTranslation()
  const { getStationById, getChargersForStation, getReviewsForStation } = useStations()
  const [sidebarOpen, setSidebarOpen] = useState(false)

  const station = id ? getStationById(id) : undefined
  const stationChargers = id ? getChargersForStation(id) : []
  const stationReviews = id ? getReviewsForStation(id) : []

  if (!station) {
    return (
      <div className="flex h-screen flex-col">
        <MobileTopBar sidebarOpen={false} onToggleSidebar={() => {}} />
        <div className="flex flex-1 items-center justify-center">
          <div className="text-center">
            <p className="text-lg font-medium text-neutral-500">{t('common.error')}</p>
            <p className="mt-1 text-sm text-neutral-400">{t('home.noStations')}</p>
          </div>
        </div>
      </div>
    )
  }

  const avgRating = stationReviews.length > 0
    ? (stationReviews.reduce((sum, r) => sum + r.rating, 0) / stationReviews.length).toFixed(1)
    : '0.0'

  return (
    <div className="flex h-screen flex-col">
      <MobileTopBar
        sidebarOpen={sidebarOpen}
        onToggleSidebar={() => setSidebarOpen(prev => !prev)}
      />
      <div className="flex-1 overflow-y-auto">
        <div className="bg-[#EAF0E6] p-6">
          <div className="mx-auto max-w-2xl">
            <h1 className="text-xl font-bold text-neutral-800">{station.name}</h1>
            <p className="mt-1 text-sm text-neutral-500">{station.address}</p>
            <div className="mt-2 flex items-center gap-4 text-sm text-neutral-500">
              <span className="flex items-center gap-1">
                <svg className="h-4 w-4 text-yellow-400" fill="currentColor" viewBox="0 0 20 20">
                  <path d="M9.049 2.927c.3-.921 1.603-.921 1.902 0l1.07 3.292a1 1 0 00.95.69h3.462c.969 0 1.371 1.24.588 1.81l-2.8 2.034a1 1 0 00-.364 1.118l1.07 3.292c.3.921-.755 1.688-1.54 1.118l-2.8-2.034a1 1 0 00-1.175 0l-2.8 2.034c-.784.57-1.838-.197-1.539-1.118l1.07-3.292a1 1 0 00-.364-1.118L2.98 8.72c-.783-.57-.38-1.81.588-1.81h3.461a1 1 0 00.951-.69l1.07-3.292z" />
                </svg>
                {avgRating} ({station.reviewCount} {t('station.reviews')})
              </span>
              <span>{station.distance} {t('station.distance')}</span>
            </div>
          </div>
        </div>

        <div className="mx-auto max-w-2xl space-y-6 px-4 py-6">
          <section>
            <h2 className="mb-3 text-base font-semibold text-neutral-700">
              {t('station.chargers')} ({station.availableCount}/{station.chargerCount} {t('station.available')})
            </h2>
            {stationChargers.length === 0 ? (
              <p className="text-sm text-neutral-400">{t('station.noChargers')}</p>
            ) : (
              <div className="space-y-2">
                {stationChargers.map(c => (
                  <ChargerRow key={c.id} charger={c} />
                ))}
              </div>
            )}
          </section>

          <section>
            <h2 className="mb-3 text-base font-semibold text-neutral-700">{t('station.reviews')}</h2>
            {stationReviews.length === 0 ? (
              <p className="text-sm text-neutral-400">{t('station.noReviews')}</p>
            ) : (
              <div className="space-y-3">
                {stationReviews.map(r => (
                  <ReviewCard key={r.id} review={r} />
                ))}
              </div>
            )}
          </section>
        </div>
      </div>
    </div>
  )
}
