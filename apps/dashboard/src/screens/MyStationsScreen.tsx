import { useTranslation } from 'react-i18next'
import { DataTable } from '../components/DataTable/DataTable'
import { mockStations } from '../mocks/stations'

export const MyStationsScreen = () => {
  const { t } = useTranslation()

  const columns = [
    { key: 'name', label: t('table.name') },
    { key: 'address', label: t('table.address') },
    { key: 'chargerCount', label: t('table.chargers') },
    { key: 'status', label: t('table.status') },
    { key: 'actions', label: t('table.actions') }
  ]

  const data = mockStations.map(station => ({
    id: station.id,
    name: station.name,
    address: station.address,
    chargerCount: station.chargerCount,
    status: station.status,
    actions: ['edit', 'manage']
  }))

  return (
    <div className="p-6">
      <h1 className="text-2xl font-bold text-text-primary mb-6">{t('dashboard.myStations')}</h1>
      <DataTable columns={columns} data={data} />
    </div>
  )
}