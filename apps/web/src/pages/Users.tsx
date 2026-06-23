import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'

const users = [
  { id: 'u-001', name: 'Emma Müller', email: 'emma@example.com', role: 'driver', status: 'active', stations: 0, joined: '2026-01-15' },
  { id: 'u-002', name: 'Liam Weber', email: 'liam@example.com', role: 'partner', status: 'active', stations: 12, joined: '2025-11-20' },
  { id: 'u-003', name: 'Sofia Keller', email: 'sofia@example.com', role: 'admin', status: 'active', stations: 0, joined: '2025-09-01' },
  { id: 'u-004', name: 'Noah Fischer', email: 'noah@example.com', role: 'driver', status: 'suspended', stations: 0, joined: '2026-03-10' },
  { id: 'u-005', name: 'Aria Huber', email: 'aria@example.com', role: 'partner', status: 'active', stations: 5, joined: '2026-04-22' },
  { id: 'u-006', name: 'Finn Schneider', email: 'finn@example.com', role: 'driver', status: 'active', stations: 0, joined: '2026-05-01' },
]

export function Users() {
  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-heading font-semibold text-foreground">Users</h1>
          <p className="mt-1 text-sm text-muted-foreground">RBAC management — {users.length} users</p>
        </div>
        <Button>Add User</Button>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-lg">All Users</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b text-left text-muted-foreground">
                  <th className="pb-3 font-medium">Name</th>
                  <th className="pb-3 font-medium">Email</th>
                  <th className="pb-3 font-medium">Role</th>
                  <th className="pb-3 font-medium">Status</th>
                  <th className="pb-3 font-medium">Stations</th>
                  <th className="pb-3 font-medium">Joined</th>
                  <th className="pb-3 font-medium" />
                </tr>
              </thead>
              <tbody>
                {users.map((u) => (
                  <tr key={u.id} className="border-b last:border-0">
                    <td className="py-3 font-medium">{u.name}</td>
                    <td className="py-3 text-muted-foreground">{u.email}</td>
                    <td className="py-3">
                      <Badge variant={u.role === 'admin' ? 'default' : u.role === 'partner' ? 'secondary' : 'outline'}>
                        {u.role}
                      </Badge>
                    </td>
                    <td className="py-3">
                      <Badge variant={u.status === 'active' ? 'success' : 'destructive'}>
                        {u.status}
                      </Badge>
                    </td>
                    <td className="py-3">{u.stations}</td>
                    <td className="py-3 text-muted-foreground">{u.joined}</td>
                    <td className="py-3">
                      <Button variant="ghost" size="sm">Edit</Button>
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
