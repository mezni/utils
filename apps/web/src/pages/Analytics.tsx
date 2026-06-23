import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'

const topEvents = [
  { name: 'session_started', count: 12450, pct: 34 },
  { name: 'session_ended', count: 11890, pct: 32 },
  { name: 'search_executed', count: 5600, pct: 15 },
  { name: 'favorite_added', count: 2100, pct: 6 },
  { name: 'filter_changed', count: 1800, pct: 5 },
  { name: 'offline_mode', count: 950, pct: 3 },
  { name: 'auth_failure', count: 420, pct: 1 },
  { name: 'payment_error', count: 210, pct: 0.6 },
]

export function Analytics() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-heading font-semibold text-foreground">Analytics</h1>
        <p className="mt-1 text-sm text-muted-foreground">Platform analytics and event telemetry</p>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle className="text-lg">Event Volume (7 days)</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-end gap-2 h-40">
              {['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'].map((day) => {
                const h = Math.random() * 70 + 20
                return (
                  <div key={day} className="flex-1 flex flex-col items-center gap-1">
                    <div
                      className="w-full rounded-t bg-gradient-to-t from-primary to-primary/40"
                      style={{ height: `${h}%` }}
                    />
                    <span className="text-xs text-muted-foreground">{day}</span>
                  </div>
                )
              })}
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-lg">Latency P95 (ms)</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              {[
                { label: 'auth-service', value: 45, threshold: 200 },
                { label: 'driver-service', value: 120, threshold: 200 },
                { label: 'admin-service', value: 180, threshold: 300 },
              ].map((svc) => (
                <div key={svc.label}>
                  <div className="flex justify-between text-sm mb-1">
                    <span>{svc.label}</span>
                    <span className={svc.value > svc.threshold ? 'text-destructive' : 'text-emerald-400'}>
                      {svc.value}ms
                    </span>
                  </div>
                  <div className="h-2 rounded-full bg-muted overflow-hidden">
                    <div
                      className={`h-full rounded-full transition-all ${svc.value > svc.threshold ? 'bg-destructive' : 'bg-emerald-500'}`}
                      style={{ width: `${(svc.value / svc.threshold) * 100}%` }}
                    />
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>

        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle className="text-lg">Event Type Distribution</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              {topEvents.map((evt) => (
                <div key={evt.name} className="flex items-center gap-3">
                  <span className="w-36 text-sm font-mono text-muted-foreground truncate">{evt.name}</span>
                  <div className="flex-1 h-6 rounded-md bg-muted overflow-hidden">
                    <div
                      className="h-full rounded-md bg-primary/70 flex items-center px-2 text-xs text-primary-foreground"
                      style={{ width: `${evt.pct * 2.5}%` }}
                    >
                      {evt.pct}%
                    </div>
                  </div>
                  <span className="w-20 text-right text-sm font-mono">{evt.count.toLocaleString()}</span>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
