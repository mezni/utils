import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'

const events = [
  { id: 'evt_001', type: 'user.login', user: 'admin@bornemap.io', ip: '192.168.1.100', time: '22 Jun 14:23:01', severity: 'info' },
  { id: 'evt_002', type: 'user.failed_login', user: 'unknown', ip: '10.0.0.55', time: '22 Jun 14:22:45', severity: 'warning' },
  { id: 'evt_003', type: 'station.create', user: 'liam@example.com', ip: '192.168.1.102', time: '22 Jun 14:20:12', severity: 'info' },
  { id: 'evt_004', type: 'station.update', user: 'aria@example.com', ip: '192.168.1.104', time: '22 Jun 14:15:33', severity: 'info' },
  { id: 'evt_005', type: 'user.role_change', user: 'admin@bornemap.io', ip: '192.168.1.100', time: '22 Jun 14:10:00', severity: 'high' },
  { id: 'evt_006', type: 'payment.failure', user: 'emma@example.com', ip: '192.168.1.200', time: '22 Jun 14:05:22', severity: 'high' },
  { id: 'evt_007', type: 'session.expired', user: 'noah@example.com', ip: '192.168.1.150', time: '22 Jun 13:55:10', severity: 'info' },
  { id: 'evt_008', type: 'station.offline', user: 'system', ip: '-', time: '22 Jun 13:45:00', severity: 'critical' },
]

const severityColors: Record<string, 'default' | 'warning' | 'destructive' | 'outline'> = {
  info: 'default',
  warning: 'warning',
  high: 'destructive',
  critical: 'destructive',
}

export function AuditLog() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-heading font-semibold text-foreground">Audit Log</h1>
        <p className="mt-1 text-sm text-muted-foreground">Security event stream and compliance trail</p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-lg">Recent Events</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b text-left text-muted-foreground">
                  <th className="pb-3 font-medium">Time</th>
                  <th className="pb-3 font-medium">Event</th>
                  <th className="pb-3 font-medium">User</th>
                  <th className="pb-3 font-medium">IP</th>
                  <th className="pb-3 font-medium">Severity</th>
                </tr>
              </thead>
              <tbody>
                {events.map((e) => (
                  <tr key={e.id} className="border-b last:border-0 hover:bg-muted/50 transition-colors">
                    <td className="py-3 font-mono text-xs text-muted-foreground">{e.time}</td>
                    <td className="py-3">
                      <span className="font-mono text-xs">{e.type}</span>
                    </td>
                    <td className="py-3 text-muted-foreground">{e.user}</td>
                    <td className="py-3 font-mono text-xs text-muted-foreground">{e.ip}</td>
                    <td className="py-3">
                      <Badge variant={severityColors[e.severity]}>{e.severity}</Badge>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
