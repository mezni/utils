import { useTranslation } from 'react-i18next'
import { useFavorites } from '../hooks/useFavorites'

interface StationCardProps {
  station: {
    id: string
    name: string
    address: string
    distance: number
    chargerCount: number
    availableCount: number
    availability: 'available' | 'unavailable'
    rating: number
    reviewCount: number
  }
  onClick: (stationId: string) => void
}

export default function StationCard({ station, onClick }: StationCardProps) {
  const { t } = useTranslation()
  const { isFavorite, toggleFavorite } = useFavorites()
  const fav = isFavorite(station.id)

  return (
    <div
      onClick={() => onClick(station.id)}
      onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onClick(station.id) } }}
      role="button"
      tabIndex={0}
      className="w-full rounded-lg bg-white p-4 text-left shadow-sm transition-shadow hover:shadow-md focus:outline-none focus:ring-2 focus:ring-brand-primary"
    >
      <div className="flex items-start justify-between">
        <div className="flex-1">
          <h3 className="text-sm font-semibold text-neutral-700">{station.name}</h3>
          <p className="mt-0.5 text-xs text-neutral-400">{station.address}</p>
        </div>
        <div className="ml-2 flex items-center gap-2">
          <span
            className={`rounded-full px-2 py-0.5 text-xs font-medium ${
              station.availability === 'available'
                ? 'bg-semantic-success/10 text-semantic-success'
                : 'bg-neutral-100 text-neutral-500'
            }`}
          >
            {station.availability === 'available' ? t('station.available') : t('station.unavailable')}
          </span>
          <button
            onClick={(e) => { e.stopPropagation(); toggleFavorite(station.id) }}
            className="focus:outline-none"
            aria-label={fav ? 'Remove from favorites' : 'Add to favorites'}
          >
            <svg
              className={`h-5 w-5 ${fav ? 'text-semantic-error' : 'text-neutral-300'}`}
              fill={fav ? 'currentColor' : 'none'}
              stroke="currentColor"
              viewBox="0 0 24 24"
            >
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4.318 6.318a4.5 4.5 0 000 6.364L12 20.364l7.682-7.682a4.5 4.5 0 00-6.364-6.364L12 7.636l-1.318-1.318a4.5 4.5 0 00-6.364 0z" />
            </svg>
          </button>
        </div>
      </div>
      <div className="mt-2 flex items-center gap-4 text-xs text-neutral-500">
        <span>{station.distance} {t('station.distance')}</span>
        <span>{station.availableCount}/{station.chargerCount} {t('station.chargers')}</span>
        <span className="flex items-center gap-0.5">
          <svg className="h-3.5 w-3.5 text-yellow-400" fill="currentColor" viewBox="0 0 20 20">
            <path d="M9.049 2.927c.3-.921 1.603-.921 1.902 0l1.07 3.292a1 1 0 00.95.69h3.462c.969 0 1.371 1.24.588 1.81l-2.8 2.034a1 1 0 00-.364 1.118l1.07 3.292c.3.921-.755 1.688-1.54 1.118l-2.8-2.034a1 1 0 00-1.175 0l-2.8 2.034c-.784.57-1.838-.197-1.539-1.118l1.07-3.292a1 1 0 00-.364-1.118L2.98 8.72c-.783-.57-.38-1.81.588-1.81h3.461a1 1 0 00.951-.69l1.07-3.292z" />
          </svg>
          {station.rating} ({station.reviewCount})
        </span>
      </div>
    </div>
  )
}
