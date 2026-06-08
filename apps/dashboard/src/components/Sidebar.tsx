import { NavLink } from 'react-router-dom';

const navItems = [
  { id: 'overview', label: 'Overview', path: '/overview' },
  { id: 'partners', label: 'Partners', path: '/partners' },
  { id: 'stations', label: 'Stations', path: '/stations' },
  { id: 'chargers', label: 'Chargers', path: '/chargers' },
];

export default function Sidebar() {
  return (
    <aside className="w-60 bg-surface border-r border-border-subtle flex flex-col h-full">
      <div className="p-4 border-b border-border-subtle">
        <h1 className="text-lg font-bold text-ink">BorneMap</h1>
        <p className="text-xs text-ink-muted">Dashboard</p>
      </div>
      <nav className="flex-1 p-2 space-y-1">
        {navItems.map((item) => (
          <NavLink
            key={item.id}
            to={item.path}
            className={({ isActive }) =>
              `flex items-center px-3 py-2 rounded-md text-sm font-medium transition-colors ${
                isActive
                  ? 'bg-moss-tint text-pine'
                  : 'text-ink-muted hover:bg-surface-muted hover:text-ink'
              }`
            }
          >
            {item.label}
          </NavLink>
        ))}
      </nav>
    </aside>
  );
}
