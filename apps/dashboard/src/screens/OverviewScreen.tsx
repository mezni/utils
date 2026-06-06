import { StatCard } from '../components/StatCard/StatCard'
import { DataCard } from '../components/DataCard/DataCard'
import { useRole } from '../hooks/useRole.ts'
import { mockPartnerReports, mockAdminReports } from '../mocks/reports'
import { mockStations } from '../mocks/stations'
import type { Report, Station } from '../types'

export const OverviewScreen = () => {
  const { role } = useRole()
  const reports = role === 'partner' ? mockPartnerReports : mockAdminReports

  return (
    <div className="p-6">
      <h1 className="text-2xl font-bold text-text-primary mb-6">Overview</h1>
      
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4 mb-6">
        {reports.map((report: Report) => (
          <StatCard
            key={report.id}
            value={report.value}
            label={report.label}
            trend={report.trend}
            trendValue={report.trendValue}
          />
        ))}
      </div>

      {role === 'admin' && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <DataCard title="Live Stations">
            <div className="space-y-2">
              {mockStations.slice(0, 5).map((station: Station) => (
                <div key={station.id} className="flex justify-between items-center p-2 bg-surface-hover rounded">
                  <span>{station.name}</span>
                  <span className={`px-2 py-1 rounded text-xs ${
                    station.status === 'available' ? 'bg-status-successBackground text-status-successText' :
                    station.status === 'in-use' ? 'bg-status-warningBackground text-status-warningText' :
                    'bg-status-errorBackground text-status-errorText'
                  }`}>
                    {station.status}
                  </span>
                </div>
              ))}
            </div>
          </DataCard>
          <DataCard title="Active Drivers">
            <div className="text-center py-8 text-text-muted">
              Real-time tracking coming soon
            </div>
          </DataCard>
        </div>
      )}

      {role === 'partner' && (
        <DataCard title="My Stations">
          <div className="space-y-2">
            {mockStations.slice(0, 3).map((station: Station) => (
              <div key={station.id} className="flex justify-between items-center p-2 bg-surface-hover rounded">
                <span>{station.name}</span>
                <span className="text-text-muted">{station.chargerCount} chargers</span>
              </div>
            ))}
          </div>
        </DataCard>
      )}
    </div>
  )
}