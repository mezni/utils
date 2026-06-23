import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'

const stations = [
  { id: 'stn_a1b2c3', name: 'Bern Bahnhof', address: 'Bahnhofstrasse 1, Bern', connectors: { ccs: 4, chademo: 2, type2: 6 }, available: 8, total: 12, status: 'active' },
  { id: 'stn_d4e5f6', name: 'Zürich Central', address: 'Bahnhofplatz 15, Zürich', connectors: { ccs: 6, chademo: 3, type2: 4 }, available: 5, total: 13, status: 'active' },
  { id: 'stn_g7h8i9', name: 'Genève Eaux-Vives', address: 'Quai Gustave-Ador, Genève', connectors: { ccs: 2, chademo: 1, type2: 3 }, available: 0, total: 6, status: 'active' },
  { id: 'stn_j0k1l2', name: 'Basel SBB', address: 'Centralbahnstrasse, Basel', connectors: { ccs: 3, chademo: 2, type2: 4 }, available: 6, total: 9, status: 'active' },
  { id: 'stn_m3n4o5', name: 'Lugano Stazione', address: 'Piazza della Stazione, Lugano', connectors: { ccs: 2, chademo: 1, type2: 2 }, available: 3, total: 5, status: 'maintenance' },
]

const typeColors: Record<string, string> = {
  ccs: 'bg-blue-500/20 text-blue-400 border-blue-500/30',
  chademo: 'bg-purple-500/20 text-purple-400 border-purple-500/30',
  type2: 'bg-cyan-500/20 text-cyan-400 border-cyan-500/30',
}

export function Stations() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-heading font-semibold text-foreground">Stations</h1>
        <p className="mt-1 text-sm text-muted-foreground">All charging stations — {stations.length} total</p>
      </div>

      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <Card>
          <CardHeader className="pb-2"><CardTitle className="text-sm font-medium text-muted-foreground">Total Stations</CardTitle></CardHeader>
          <CardContent><div className="text-2xl font-bold font-heading">1,234</div></CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2"><CardTitle className="text-sm font-medium text-muted-foreground">Active</CardTitle></CardHeader>
          <CardContent><div className="text-2xl font-bold font-heading text-emerald-400">1,198</div></CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2"><CardTitle className="text-sm font-medium text-muted-foreground">Maintenance</CardTitle></CardHeader>
          <CardContent><div className="text-2xl font-bold font-heading text-amber-400">28</div></CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2"><CardTitle className="text-sm font-medium text-muted-foreground">Offline</CardTitle></CardHeader>
          <CardContent><div className="text-2xl font-bold font-heading text-destructive">8</div></CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-lg">Station List</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          {stations.map((s) => (
            <div key={s.id} className="rounded-lg border p-4">
              <div className="flex items-start justify-between">
                <div>
                  <div className="flex items-center gap-2">
                    <span className="font-medium">{s.name}</span>
                    <Badge variant={s.status === 'active' ? 'success' : 'warning'}>{s.status}</Badge>
                  </div>
                  <p className="mt-0.5 text-sm text-muted-foreground">{s.address}</p>
                  <p className="mt-0.5 text-xs text-muted-foreground font-mono">{s.id}</p>
                </div>
                <div className="text-right">
                  <p className="text-lg font-bold font-heading">{s.available}<span className="text-sm font-normal text-muted-foreground">/{s.total}</span></p>
                  <p className="text-xs text-muted-foreground">available</p>
                </div>
              </div>
              <div className="mt-3 flex gap-2">
                {Object.entries(s.connectors).map(([type, count]) => (
                  <span key={type} className={`rounded-full border px-2.5 py-0.5 text-xs font-medium ${typeColors[type]}`}>
                    {type.toUpperCase()} × {count}
                  </span>
                ))}
              </div>
            </div>
          ))}
        </CardContent>
      </Card>
    </div>
  )
}
