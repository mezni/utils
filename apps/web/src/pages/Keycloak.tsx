import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'

const clients = [
  { id: 'borne-map-web', type: 'public', protocol: 'openid-connect', access: 'public', status: 'enabled' },
  { id: 'borne-map-mobile', type: 'public', protocol: 'openid-connect', access: 'public', status: 'enabled' },
  { id: 'admin-cli', type: 'confidential', protocol: 'openid-connect', access: 'confidential', status: 'enabled' },
  { id: 'realm-management', type: 'confidential', protocol: 'openid-connect', access: 'confidential', status: 'enabled' },
]

const roles = [
  { name: 'driver', users: 1842, description: 'Standard EV driver — map, search, favorites, preferences' },
  { name: 'partner', users: 89, description: 'Station operator — manage own stations, view analytics' },
  { name: 'admin', users: 5, description: 'System administrator — full platform access' },
]

const idps = [
  { name: 'Google', protocol: 'OIDC', status: 'enabled' },
  { name: 'Apple', protocol: 'OIDC', status: 'enabled' },
  { name: 'Microsoft', protocol: 'OIDC', status: 'disabled' },
]

export function Keycloak() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-heading font-semibold text-foreground">Keycloak</h1>
        <p className="mt-1 text-sm text-muted-foreground">SSO realm configuration and identity management</p>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle className="text-lg">Realm: borne-map</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="grid grid-cols-2 gap-3">
              {[
                { label: 'Users', value: '1,936' },
                { label: 'Clients', value: '4' },
                { label: 'Roles', value: '3' },
                { label: 'IDPs', value: '3' },
              ].map((s) => (
                <div key={s.label} className="rounded-lg border p-3 text-center">
                  <p className="text-2xl font-bold font-heading">{s.value}</p>
                  <p className="text-xs text-muted-foreground">{s.label}</p>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-lg">Clients</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2">
            {clients.map((c) => (
              <div key={c.id} className="flex items-center justify-between rounded-lg border p-3">
                <div>
                  <p className="text-sm font-medium">{c.id}</p>
                  <p className="text-xs text-muted-foreground">{c.protocol} · {c.access}</p>
                </div>
                <Badge variant="success">{c.status}</Badge>
              </div>
            ))}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-lg">Roles</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2">
            {roles.map((r) => (
              <div key={r.name} className="rounded-lg border p-3">
                <div className="flex items-center justify-between">
                  <span className="font-medium text-sm">{r.name}</span>
                  <Badge variant="outline">{r.users} users</Badge>
                </div>
                <p className="mt-1 text-xs text-muted-foreground">{r.description}</p>
              </div>
            ))}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-lg">Identity Providers</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2">
            {idps.map((idp) => (
              <div key={idp.name} className="flex items-center justify-between rounded-lg border p-3">
                <div className="flex items-center gap-2">
                  <span className="font-medium text-sm">{idp.name}</span>
                  <span className="text-xs text-muted-foreground">{idp.protocol}</span>
                </div>
                <Badge variant={idp.status === 'enabled' ? 'success' : 'secondary'}>{idp.status}</Badge>
              </div>
            ))}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
