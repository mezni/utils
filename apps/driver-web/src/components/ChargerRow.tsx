import { useTranslation } from 'react-i18next'
import type { Charger } from '../types'

interface ChargerRowProps {
  charger: Charger
}

const connectorIcons: Record<string, string> = {
  Type2: 'Type 2',
  CCS: 'CCS',
  CHAdeMO: 'CHAdeMO',
}

export default function ChargerRow({ charger }: ChargerRowProps) {
  const { t } = useTranslation()

  return (
    <div className="flex items-center justify-between rounded-lg border border-neutral-200 bg-white px-4 py-3">
      <div className="flex items-center gap-3">
        <span className="rounded bg-neutral-100 px-2 py-0.5 text-xs font-semibold text-neutral-600">
          {connectorIcons[charger.connectorType]}
        </span>
        <div>
          <p className="text-sm font-medium text-neutral-700">{charger.powerKw} kW</p>
          <p className="text-xs text-neutral-400">{charger.pricePerKwh.toFixed(2)} {t('station.pricePerKwh')}</p>
        </div>
      </div>
      <span
        className={`rounded-full px-2 py-0.5 text-xs font-medium ${
          charger.availability === 'available'
            ? 'bg-semantic-success/10 text-semantic-success'
            : 'bg-neutral-100 text-neutral-500'
        }`}
      >
        {charger.availability === 'available' ? t('station.available') : t('station.unavailable')}
      </span>
    </div>
  )
}
