import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api } from '@/lib/api'
import { Skeleton } from '@/components/ui/Skeleton'
import { motion } from 'framer-motion'

interface User {
  id: string
  email: string
  role: 'admin' | 'user'
  created_at: string
  last_login: string | null
  is_active: boolean
}

interface UsersResponse {
  users: User[]
  total: number
  page: number
  per_page: number
}

type SortField = 'email' | 'created_at' | 'role'
type SortDir = 'asc' | 'desc'

export function UsersPage() {
  const [search, setSearch] = useState('')
  const [page, setPage] = useState(1)
  const [sortField, setSortField] = useState<SortField>('created_at')
  const [sortDir, setSortDir] = useState<SortDir>('desc')

  const { data, isLoading } = useQuery<UsersResponse>({
    queryKey: ['users', search, page, sortField, sortDir],
    queryFn: async () => {
      const res = await api.get<UsersResponse>('/admin/users', {
        params: { search, page, per_page: 10, sort_field: sortField, sort_dir: sortDir },
      })
      return res.data
    },
  })

  const toggleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'))
    } else {
      setSortField(field)
      setSortDir('asc')
    }
  }

  const SortIcon = ({ field }: { field: SortField }) => {
    if (sortField !== field) {
      return (
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="opacity-30" aria-hidden="true">
          <path d="m7 15 5 5 5-5" />
          <path d="m7 9 5-5 5 5" />
        </svg>
      )
    }
    return (
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
        {sortDir === 'asc' ? (
          <path d="m18 15-6-6-6 6" />
        ) : (
          <path d="m6 9 6 6 6-6" />
        )}
      </svg>
    )
  }

  const totalPages = data ? Math.ceil(data.total / data.per_page) : 0

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold text-foreground font-heading">Users</h2>
        <div className="relative">
          <svg
            width="16"
            height="16"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            className="absolute left-3 top-1/2 -translate-y-1/2 text-foreground/40"
            aria-hidden="true"
          >
            <circle cx="11" cy="11" r="8" />
            <path d="m21 21-4.3-4.3" />
          </svg>
          <input
            type="search"
            placeholder="Search users..."
            value={search}
            onChange={(e) => { setSearch(e.target.value); setPage(1) }}
            className="w-64 pl-9 pr-3 py-2 rounded-lg border border-border bg-background text-sm text-foreground focus:outline-none focus:ring-2 focus:ring-ring transition-colors duration-150"
            aria-label="Search users"
          />
        </div>
      </div>

      <div className="overflow-x-auto rounded-xl border border-border">
        <table className="w-full text-sm" role="grid" aria-label="Users table">
          <thead>
            <tr className="border-b border-border bg-muted/20">
              <Th sortable field="email" current={sortField} dir={sortDir} onClick={() => toggleSort('email')}>
                Email <SortIcon field="email" />
              </Th>
              <Th sortable field="role" current={sortField} dir={sortDir} onClick={() => toggleSort('role')}>
                Role <SortIcon field="role" />
              </Th>
              <Th sortable field="created_at" current={sortField} dir={sortDir} onClick={() => toggleSort('created_at')}>
                Created <SortIcon field="created_at" />
              </Th>
              <Th>Status</Th>
            </tr>
          </thead>
          <tbody>
            {isLoading ? (
              Array.from({ length: 5 }).map((_, i) => (
                <tr key={i} className="border-b border-border last:border-0">
                  <td colSpan={4} className="px-4 py-3"><Skeleton className="h-5 w-full" /></td>
                </tr>
              ))
            ) : data?.users.length === 0 ? (
              <tr>
                <td colSpan={4} className="px-4 py-12 text-center text-foreground/50">
                  No users found
                </td>
              </tr>
            ) : (
              data?.users.map((user, i) => (
                <motion.tr
                  key={user.id}
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  transition={{ duration: 0.15, delay: i * 0.03 }}
                  className="border-b border-border last:border-0 hover:bg-muted/10 transition-colors duration-100"
                >
                  <td className="px-4 py-3 font-medium text-foreground">{user.email}</td>
                  <td className="px-4 py-3">
                    <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${
                      user.role === 'admin'
                        ? 'bg-primary/10 text-primary'
                        : 'bg-muted text-foreground/60'
                    }`}>
                      {user.role}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-foreground/60">
                    {new Date(user.created_at).toLocaleDateString()}
                  </td>
                  <td className="px-4 py-3">
                    <span className={`inline-flex items-center gap-1.5 text-xs font-medium ${
                      user.is_active ? 'text-primary' : 'text-foreground/40'
                    }`}>
                      <span className={`w-1.5 h-1.5 rounded-full ${
                        user.is_active ? 'bg-primary' : 'bg-foreground/30'
                      }`} aria-hidden="true" />
                      {user.is_active ? 'Active' : 'Inactive'}
                    </span>
                  </td>
                </motion.tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {totalPages > 1 && (
        <div className="flex items-center justify-between" role="navigation" aria-label="Pagination">
          <span className="text-xs text-foreground/50">
            Page {page} of {totalPages}
          </span>
          <div className="flex gap-2">
            <PageButton
              disabled={page <= 1}
              onClick={() => setPage((p) => p - 1)}
            >
              Previous
            </PageButton>
            <PageButton
              disabled={page >= totalPages}
              onClick={() => setPage((p) => p + 1)}
            >
              Next
            </PageButton>
          </div>
        </div>
      )}
    </div>
  )
}

interface ThProps {
  children: React.ReactNode
  sortable?: boolean
  field?: SortField
  current?: SortField
  dir?: SortDir
  onClick?: () => void
}

function Th({ children, sortable, onClick }: ThProps) {
  const content = (
    <div className={`flex items-center gap-1 ${sortable ? 'cursor-pointer hover:text-foreground' : ''}`}>
      {children}
    </div>
  )

  return (
    <th
      className="px-4 py-3 text-left text-xs font-medium text-foreground/50 uppercase tracking-wider"
      onClick={sortable ? onClick : undefined}
      aria-sort={sortable ? undefined : undefined}
    >
      {sortable ? content : children}
    </th>
  )
}

function PageButton({ children, disabled, onClick }: { children: React.ReactNode; disabled: boolean; onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      className="px-3 py-1.5 text-xs font-medium rounded-md border border-border text-foreground/60 hover:text-foreground hover:bg-muted/50 transition-colors duration-150 cursor-pointer disabled:opacity-30 disabled:cursor-not-allowed"
    >
      {children}
    </button>
  )
}
