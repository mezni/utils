import { useTranslation } from 'react-i18next'

type ConnectorType = 'all' | 'Type2' | 'CCS' | 'CHAdeMO'
type AvailabilityFilter = 'all' | 'available'

interface FilterPillsProps {
  selectedChargerType: ConnectorType
  onChargerTypeChange: (type: ConnectorType) => void
  selectedAvailability: AvailabilityFilter
  onAvailabilityChange: (filter: AvailabilityFilter) => void
}

const chargerTypes: ConnectorType[] = ['all', 'Type2', 'CCS', 'CHAdeMO']

export default function FilterPills({
  selectedChargerType,
  onChargerTypeChange,
  selectedAvailability,
  onAvailabilityChange,
}: FilterPillsProps) {
  const { t } = useTranslation()

  return (
    <div className="mx-4 my-2 space-y-2">
      <div className="flex flex-wrap gap-2">
        {chargerTypes.map(type => (
          <button
            key={type}
            onClick={() => onChargerTypeChange(type)}
            className={`rounded-full px-3 py-1 text-xs font-medium transition-colors ${
              selectedChargerType === type
                ? 'bg-brand-primary text-white'
                : 'bg-neutral-100 text-neutral-600 hover:bg-neutral-200'
            }`}
            aria-pressed={selectedChargerType === type}
          >
            {type === 'all' ? t('search.all') : t(`charger.${type.toLowerCase()}`)}
          </button>
        ))}
      </div>
      <div className="flex flex-wrap gap-2">
        <button
          onClick={() => onAvailabilityChange('all')}
          className={`rounded-full px-3 py-1 text-xs font-medium transition-colors ${
            selectedAvailability === 'all'
              ? 'bg-brand-primary text-white'
              : 'bg-neutral-100 text-neutral-600 hover:bg-neutral-200'
          }`}
          aria-pressed={selectedAvailability === 'all'}
        >
          {t('search.all')}
        </button>
        <button
          onClick={() => onAvailabilityChange('available')}
          className={`rounded-full px-3 py-1 text-xs font-medium transition-colors ${
            selectedAvailability === 'available'
              ? 'bg-semantic-success text-white'
              : 'bg-neutral-100 text-neutral-600 hover:bg-neutral-200'
          }`}
          aria-pressed={selectedAvailability === 'available'}
        >
          {t('search.available')}
        </button>
      </div>
    </div>
  )
}
