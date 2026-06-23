import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'

const kpis = [
  { label: 'Active Users', value: '2,847', change: '+12%', icon: 'Users', color: 'text-blue-400' },
  { label: 'Total Stations', value: '1,234', change: '+8%', icon: 'Zap', color: 'text-emerald-400' },
  { label: 'Today\'s Sessions', value: '892', change: '+23%', icon: 'BarChart3', color: 'text-amber-400' },
  { label: 'Avg Response', value: '47ms', change: '-5ms', icon: 'Server', color: 'text-purple-400' },
]

const services = [
  { name: 'auth-service', status: 'healthy', uptime: '99.99%', port: 3000 },
  { name: 'driver-service', status: 'healthy', uptime: '99.97%', port: 3001 },
  { name: 'admin-service', status: 'degraded', uptime: '99.85%', port: 3002 },
]

function KpiIcon({ name, className }: { name: string; className?: string }) {
  return (
    <svg className={`size-5 ${className}`} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      {name === 'Users' && <><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2" /><circle cx="9" cy="7" r="4" /><path d="M22 21v-2a4 4 0 0 0-3-3.87" /><path d="M16 3.13a4 4 0 0 1 0 7.75" /></>}
      {name === 'Zap' && <><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2" /></>}
      {name === 'BarChart3' && <><line x1="12" y1="20" x2="12" y2="10" /><line x1="18" y1="20" x2="18" y2="4" /><line x1="6" y1="20" x2="6" y2="16" /></>}
      {name === 'Server' && <><rect x="3" y="4" width="18" height="8" rx="2" /><rect x="3" y="16" width="18" height="4" rx="1" /><path d="M7 8h.01M7 18h.01" /></>}
    </svg>
  )
}

export function Overview() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-heading font-semibold text-foreground">Overview</h1>
        <p className="mt-1 text-sm text-muted-foreground">System health and global KPIs</p>
      </div>

      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        {kpis.map((kpi) => (
          <Card key={kpi.label}>
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">{kpi.label}</CardTitle>
              <KpiIcon name={kpi.icon} className={kpi.color} />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold font-heading">{kpi.value}</div>
              <p className={`text-xs ${kpi.change.startsWith('+') ? 'text-emerald-400' : 'text-amber-400'}`}>
                {kpi.change} from yesterday
              </p>
            </CardContent>
          </Card>
        ))}
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle className="text-lg">Service Health</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            {services.map((svc) => (
              <div key={svc.name} className="flex items-center justify-between rounded-lg border p-3">
                <div className="flex items-center gap-3">
                  <div className={`size-2.5 rounded-full ${svc.status === 'healthy' ? 'bg-emerald-500' : 'bg-amber-500'}`} />
                  <div>
                    <p className="text-sm font-medium">{svc.name}</p>
                    <p className="text-xs text-muted-foreground">Port {svc.port}</p>
                  </div>
                </div>
                <Badge variant={svc.status === 'healthy' ? 'success' : 'warning'}>
                  {svc.uptime}
                </Badge>
              </div>
            ))}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-lg">Event Volume (Last 24h)</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-end gap-1 h-32">
              {Array.from({ length: 24 }, (_, i) => {
                const h = Math.random() * 80 + 10
                return (
                  <div key={i} className="flex-1 flex flex-col items-center gap-1">
                    <div
                      className="w-full rounded-t bg-primary/60 hover:bg-primary/80 transition-colors"
                      style={{ height: `${h}%` }}
                    />
                    <span className="text-[10px] text-muted-foreground">{i}h</span>
                  </div>
                )
              })}
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
