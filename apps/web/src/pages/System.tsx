import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'

const dbs = [
  { name: 'platform_db', engine: 'PostgreSQL 16', size: '2.4 GB', connections: 23, status: 'healthy' },
  { name: 'analytics_db', engine: 'PostgreSQL 16', size: '8.1 GB', connections: 12, status: 'healthy' },
  { name: 'keycloak_db', engine: 'PostgreSQL 16', size: '480 MB', connections: 8, status: 'healthy' },
]

const caches = [
  { name: 'JWKS Cache', type: 'In-Memory', entries: 3, ttl: '3600s', status: 'healthy' },
  { name: 'Session Cache', type: 'Redis', entries: 1423, ttl: '1800s', status: 'healthy' },
  { name: 'Rate Limit', type: 'In-Memory', entries: 89, ttl: '60s', status: 'healthy' },
]

export function System() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-heading font-semibold text-foreground">System</h1>
        <p className="mt-1 text-sm text-muted-foreground">Service health, databases, and infrastructure</p>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle className="text-lg">Database Pool</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            {dbs.map((db) => (
              <div key={db.name} className="rounded-lg border p-3">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <div className={`size-2 rounded-full ${db.status === 'healthy' ? 'bg-emerald-500' : 'bg-destructive'}`} />
                    <span className="font-medium text-sm">{db.name}</span>
                  </div>
                  <Badge variant="success">{db.status}</Badge>
                </div>
                <div className="mt-2 grid grid-cols-3 gap-2 text-xs text-muted-foreground">
                  <span>{db.engine}</span>
                  <span>{db.size}</span>
                  <span>{db.connections} connections</span>
                </div>
              </div>
            ))}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-lg">Cache Status</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            {caches.map((c) => (
              <div key={c.name} className="rounded-lg border p-3">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <div className={`size-2 rounded-full ${c.status === 'healthy' ? 'bg-emerald-500' : 'bg-destructive'}`} />
                    <span className="font-medium text-sm">{c.name}</span>
                  </div>
                  <Badge variant="success">{c.status}</Badge>
                </div>
                <div className="mt-2 grid grid-cols-3 gap-2 text-xs text-muted-foreground">
                  <span>{c.type}</span>
                  <span>{c.entries} entries</span>
                  <span>TTL: {c.ttl}</span>
                </div>
              </div>
            ))}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-lg">Recent Migrations</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-2 text-sm">
              {[
                { name: '0007_enable_pg_trgm', status: 'applied', date: '2026-06-22' },
                { name: '0006_add_preferences_jsonb', status: 'applied', date: '2026-06-21' },
                { name: '0005_add_telemetry_events', status: 'applied', date: '2026-06-20' },
                { name: '0004_extend_audit_log', status: 'applied', date: '2026-06-15' },
              ].map((m) => (
                <div key={m.name} className="flex items-center justify-between rounded border p-2.5">
                  <div className="flex items-center gap-2">
                    <div className="size-2 rounded-full bg-emerald-500" />
                    <span className="font-mono text-xs">{m.name}</span>
                  </div>
                  <span className="text-xs text-muted-foreground">{m.date}</span>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-lg">Resource Usage</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {[
              { label: 'CPU', used: 34, total: 100, color: 'bg-blue-500' },
              { label: 'Memory', used: 2.1, total: 8, unit: 'GB', color: 'bg-purple-500' },
              { label: 'Disk', used: 45, total: 200, unit: 'GB', color: 'bg-emerald-500' },
            ].map((r) => (
              <div key={r.label}>
                <div className="flex justify-between text-sm mb-1">
                  <span>{r.label}</span>
                  <span className="text-muted-foreground">{r.used}{r.unit || '%'} / {r.total}{r.unit || '%'}</span>
                </div>
                <div className="h-2.5 rounded-full bg-muted overflow-hidden">
                  <div
                    className={`h-full rounded-full ${r.color} transition-all`}
                    style={{ width: `${(r.used / r.total) * 100}%` }}
                  />
                </div>
              </div>
            ))}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
