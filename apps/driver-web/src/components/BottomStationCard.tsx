import { useTranslation } from 'react-i18next'

interface BottomStationCardProps {
  station: {
    id: string
    name: string
    address: string
    availability: 'available' | 'unavailable'
    distance: number
    chargerCount: number
    availableCount: number
    rating: number
  }
  specs?: Array<{ label: string; value: string }>
  onClick: (stationId: string) => void
  onNavigate?: (stationId: string) => void
}

export default function BottomStationCard({ station, specs = [], onClick, onNavigate }: BottomStationCardProps) {
  const { t } = useTranslation()

  return (
    <div
      onClick={() => onClick(station.id)}
      onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onClick(station.id) } }}
      role="button"
      tabIndex={0}
      className="w-full rounded-t-xl bg-white p-4 shadow-lg"
    >
      <div className="flex items-start justify-between">
        <div className="flex-1">
          <h3 className="text-sm font-semibold text-neutral-700">{station.name}</h3>
          <p className="text-xs text-neutral-400">{station.address}</p>
        </div>
        <span
          className={`ml-2 rounded-full px-2 py-0.5 text-xs font-medium ${
            station.availability === 'available'
              ? 'bg-semantic-success/10 text-semantic-success'
              : 'bg-neutral-100 text-neutral-500'
          }`}
        >
          {station.availability === 'available' ? t('station.available') : t('station.unavailable')}
        </span>
      </div>
      <div className="mt-2 flex items-center gap-3 text-xs text-neutral-500">
        <span>{station.distance} {t('station.distance')}</span>
        <span>{station.availableCount}/{station.chargerCount} {t('station.chargers')}</span>
        <span className="flex items-center gap-0.5">
          <svg className="h-3.5 w-3.5 text-yellow-400" fill="currentColor" viewBox="0 0 20 20">
            <path d="M9.049 2.927c.3-.921 1.603-.921 1.902 0l1.07 3.292a1 1 0 00.95.69h3.462c.969 0 1.371 1.24.588 1.81l-2.8 2.034a1 1 0 00-.364 1.118l1.07 3.292c.3.921-.755 1.688-1.54 1.118l-2.8-2.034a1 1 0 00-1.175 0l-2.8 2.034c-.784.57-1.838-.197-1.539-1.118l1.07-3.292a1 1 0 00-.364-1.118L2.98 8.72c-.783-.57-.38-1.81.588-1.81h3.461a1 1 0 00.951-.69l1.07-3.292z" />
          </svg>
          {station.rating}
        </span>
      </div>
      {specs.length > 0 && (
        <div className="mt-2 border-t border-neutral-100 pt-2 text-xs text-neutral-500">
          {specs.map((spec, i) => (
            <div key={i} className="flex justify-between">
              <span>{spec.label}</span>
              <span className="font-medium text-neutral-700">{spec.value}</span>
            </div>
          ))}
        </div>
      )}
      {onNavigate && (
        <button
          onClick={(e) => { e.stopPropagation(); onNavigate(station.id) }}
          className="mt-2 w-full rounded-md bg-brand-primary py-1.5 text-xs font-medium text-white"
        >
          {t('station.directions')}
        </button>
      )}
    </div>
  )
}
