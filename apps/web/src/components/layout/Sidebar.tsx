import { cn } from '@/lib/utils'

interface NavItem {
  label: string
  icon: string
  value: string
}

const navItems: NavItem[] = [
  { label: 'Overview', icon: 'LayoutDashboard', value: 'overview' },
  { label: 'Users', icon: 'Users', value: 'users' },
  { label: 'Stations', icon: 'Zap', value: 'stations' },
  { label: 'Analytics', icon: 'BarChart3', value: 'analytics' },
  { label: 'System', icon: 'Server', value: 'system' },
  { label: 'Audit Log', icon: 'ScrollText', value: 'audit' },
  { label: 'Keycloak', icon: 'Shield', value: 'keycloak' },
]

function Icon({ name, className }: { name: string; className?: string }) {
  return (
    <svg className={cn('size-5', className)} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      {name === 'LayoutDashboard' && <><rect x="3" y="3" width="7" height="9" /><rect x="14" y="3" width="7" height="5" /><rect x="14" y="12" width="7" height="9" /><rect x="3" y="16" width="7" height="5" /></>}
      {name === 'Users' && <><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2" /><circle cx="9" cy="7" r="4" /><path d="M22 21v-2a4 4 0 0 0-3-3.87" /><path d="M16 3.13a4 4 0 0 1 0 7.75" /></>}
      {name === 'Zap' && <><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2" /></>}
      {name === 'BarChart3' && <><line x1="12" y1="20" x2="12" y2="10" /><line x1="18" y1="20" x2="18" y2="4" /><line x1="6" y1="20" x2="6" y2="16" /></>}
      {name === 'Server' && <><rect x="3" y="4" width="18" height="8" rx="2" /><rect x="3" y="16" width="18" height="4" rx="1" /><path d="M7 8h.01M7 18h.01" /></>}
      {name === 'ScrollText' && <><path d="M15 3H5a2 2 0 0 0-2 2v14c0 1.1.9 2 2 2h14a2 2 0 0 0 2-2V8l-5-5Z" /><path d="M15 3v5h5" /><path d="M8 12h8" /><path d="M8 16h6" /></>}
      {name === 'Shield' && <><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" /></>}
      {name === 'ChevronLeft' && <><path d="m15 18-6-6 6-6" /></>}
    </svg>
  )
}

interface SidebarProps {
  active: string
  onNavigate: (value: string) => void
  collapsed: boolean
  onToggle: () => void
}

export function Sidebar({ active, onNavigate, collapsed, onToggle }: SidebarProps) {
  return (
    <aside className={cn(
      'flex flex-col border-r bg-sidebar transition-all duration-300',
      collapsed ? 'w-16' : 'w-60',
    )}>
      <div className="flex h-14 items-center border-b px-4">
        {!collapsed && (
          <span className="font-heading text-lg font-semibold text-sidebar-foreground">BorneMap</span>
        )}
        <button
          onClick={onToggle}
          className={cn(
            'ml-auto rounded-md p-1.5 text-sidebar-foreground/60 hover:text-sidebar-foreground transition-colors',
            collapsed && 'mx-auto',
          )}
        >
          <Icon name="ChevronLeft" className={cn('transition-transform', collapsed && 'rotate-180')} />
        </button>
      </div>
      <nav className="flex-1 space-y-1 p-2">
        {navItems.map((item) => (
          <button
            key={item.value}
            onClick={() => onNavigate(item.value)}
            className={cn(
              'flex w-full items-center gap-3 rounded-md px-3 py-2.5 text-sm font-medium transition-colors',
              active === item.value
                ? 'bg-sidebar-active text-white'
                : 'text-sidebar-foreground/70 hover:bg-sidebar-muted hover:text-sidebar-foreground',
              collapsed && 'justify-center px-0',
            )}
            title={collapsed ? item.label : undefined}
          >
            <Icon name={item.icon} />
            {!collapsed && <span>{item.label}</span>}
          </button>
        ))}
      </nav>
      <div className="border-t p-4">
        {!collapsed && (
          <div className="flex items-center gap-3">
            <div className="flex size-8 items-center justify-center rounded-full bg-primary text-xs font-bold text-primary-foreground">
              A
            </div>
            <div className="flex-1 min-w-0">
              <p className="text-sm font-medium text-sidebar-foreground truncate">admin</p>
              <p className="text-xs text-sidebar-foreground/50 truncate">admin@bornemap.io</p>
            </div>
          </div>
        )}
      </div>
    </aside>
  )
}
