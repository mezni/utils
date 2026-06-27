import { type ReactNode } from 'react'
import { NavLink } from 'react-router-dom'

interface NavItem {
  to: string
  label: string
  icon: ReactNode
}

const navItems: NavItem[] = [
  {
    to: '/',
    label: 'Dashboard',
    icon: (
      <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
        <rect x="3" y="3" width="7" height="7" />
        <rect x="14" y="3" width="7" height="7" />
        <rect x="14" y="14" width="7" height="7" />
        <rect x="3" y="14" width="7" height="7" />
      </svg>
    ),
  },
  {
    to: '/users',
    label: 'Users',
    icon: (
      <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
        <path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2" />
        <circle cx="9" cy="7" r="4" />
        <path d="M22 21v-2a4 4 0 0 0-3-3.87" />
        <path d="M16 3.13a4 4 0 0 1 0 7.75" />
      </svg>
    ),
  },
]

interface SidebarProps {
  collapsed: boolean
  onToggle: () => void
}

export function Sidebar({ collapsed, onToggle }: SidebarProps) {
  return (
    <aside
      className={`fixed left-0 top-0 z-40 h-screen bg-muted border-r border-border transition-all duration-200 ${
        collapsed ? 'w-16' : 'w-60'
      }`}
      role="navigation"
      aria-label="Main navigation"
    >
      <div className="flex items-center justify-between h-16 px-4 border-b border-border">
        {!collapsed && (
          <span className="font-heading text-foreground text-lg font-semibold tracking-wider">
            BorneMap
          </span>
        )}
        <button
          onClick={onToggle}
          className="p-2 rounded-md text-foreground/60 hover:text-primary hover:bg-primary/10 transition-colors duration-150 cursor-pointer"
          aria-label={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
        >
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
            {collapsed ? (
              <polyline points="9 18 15 12 9 6" />
            ) : (
              <polyline points="15 18 9 12 15 6" />
            )}
          </svg>
        </button>
      </div>

      <nav className="p-2 space-y-1" role="list">
        {navItems.map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            end={item.to === '/'}
            className={({ isActive }) =>
              `flex items-center gap-3 px-3 py-2.5 rounded-md text-sm font-medium transition-colors duration-150 ${
                isActive
                  ? 'bg-primary/10 text-primary'
                  : 'text-foreground/60 hover:text-foreground hover:bg-muted/80'
              }`
            }
            role="listitem"
            aria-label={item.label}
          >
            {item.icon}
            {!collapsed && <span>{item.label}</span>}
          </NavLink>
        ))}
      </nav>
    </aside>
  )
}
