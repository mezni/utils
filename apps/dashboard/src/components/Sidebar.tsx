import { NavLink } from 'react-router-dom';

const navItems = [
  { id: 'overview', label: 'Overview', path: '/overview' },
  { id: 'partners', label: 'Partners', path: '/partners' },
  { id: 'stations', label: 'Stations', path: '/stations' },
  { id: 'chargers', label: 'Chargers', path: '/chargers' },
];

export default function Sidebar() {
  return (
    <aside className="w-60 bg-white border-r border-gray-200 flex flex-col h-full">
      <div className="p-4 border-b border-gray-200">
        <h1 className="text-lg font-bold text-gray-800">BorneMap</h1>
        <p className="text-xs text-gray-500">Dashboard</p>
      </div>
      <nav className="flex-1 p-2 space-y-1">
        {navItems.map((item) => (
          <NavLink
            key={item.id}
            to={item.path}
            className={({ isActive }) =>
              `flex items-center px-3 py-2 rounded-md text-sm font-medium transition-colors ${
                isActive
                  ? 'bg-[#EAF0E6] text-[#007943]'
                  : 'text-gray-600 hover:bg-gray-50 hover:text-gray-800'
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
