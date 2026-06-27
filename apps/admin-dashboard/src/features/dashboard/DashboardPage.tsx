import { useQuery } from '@tanstack/react-query'
import { api } from '@/lib/api'
import { MetricsCard } from './MetricsCard'
import { UserGrowthChart } from './UserGrowthChart'
import { MetricsCardSkeleton } from '@/components/ui/Skeleton'

interface UsersGrowthPoint {
  date: string
  count: number
}

interface DashboardMetrics {
  total: number
  growth: UsersGrowthPoint[]
}

function UsersIcon() {
  return (
    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2" />
      <circle cx="9" cy="7" r="4" />
    </svg>
  )
}

function ActiveIcon() {
  return (
    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <path d="M22 12h-4l-3 9L9 3l-3 9H2" />
    </svg>
  )
}

function TodayIcon() {
  return (
    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <rect x="3" y="4" width="18" height="18" rx="2" ry="2" />
      <line x1="16" y1="2" x2="16" y2="6" />
      <line x1="8" y1="2" x2="8" y2="6" />
      <line x1="3" y1="10" x2="21" y2="10" />
    </svg>
  )
}

function TrackerIcon() {
  return (
    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <circle cx="12" cy="12" r="10" />
      <polyline points="12 6 12 12 16 14" />
    </svg>
  )
}

export function DashboardPage() {
  const { data: metrics, isLoading } = useQuery<DashboardMetrics>({
    queryKey: ['dashboard', 'metrics'],
    queryFn: async () => {
      const res = await api.get<DashboardMetrics>('/api/v1/admin/metrics/users', {
        params: { range: '30d' }
      })
      return res.data
    },
    refetchInterval: 30_000,
  })

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        {isLoading ? (
          <>
            <MetricsCardSkeleton />
            <MetricsCardSkeleton />
            <MetricsCardSkeleton />
            <MetricsCardSkeleton />
          </>
        ) : (
          <>
            <MetricsCard
              label="Total Users"
              value={metrics?.total ?? 0}
              change="+12%"
              changeType="positive"
              icon={<UsersIcon />}
            />
            <MetricsCard
              label="Active Users"
              value={0}
              change="Coming Soon"
              changeType="neutral"
              icon={<ActiveIcon />}
            />
            <MetricsCard
              label="New Today"
              value={0}
              change="Coming Soon"
              icon={<TodayIcon />}
            />
            <MetricsCard
              label="Trackers"
              value={0}
              change="Coming Soon"
              changeType="neutral"
              icon={<TrackerIcon />}
            />
          </>
        )}
      </div>

      <UserGrowthChart 
        data={metrics?.growth.map(point => ({
          date: new Date(point.date).toLocaleDateString('en-US', { month: 'short' }),
          users: point.count
        })) || []} 
      />
    </div>
  )
}
