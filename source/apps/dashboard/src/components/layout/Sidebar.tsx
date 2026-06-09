import { useEffect, useState } from 'react';
import { useRole } from '../../context/RoleContext';
import { NavigationItem } from './NavigationItem';
import { list } from '../../api/client';

const adminNav = [
  { to: '/', label: 'Overview', icon: '📊' },
  { to: '/partners', label: 'Partners', icon: '🏢' },
  { to: '/stations', label: 'Stations', icon: '⚡' },
  { to: '/chargers', label: 'Chargers', icon: '🔌' },
];

const partnerNav = [
  { to: '/', label: 'Overview', icon: '📊' },
  { to: '/my-stations', label: 'My Stations', icon: '⚡' },
  { to: '/my-chargers', label: 'My Chargers', icon: '🔌' },
  { to: '/availability', label: 'Availability', icon: '📋' },
];

interface Partner {
  id: string;
  name: string;
}

export function Sidebar() {
  const { role, setRole, selectedPartnerId, setSelectedPartnerId } = useRole();
  const [partners, setPartners] = useState<Partner[]>([]);

  const nav = role === 'admin' ? adminNav : partnerNav;

  useEffect(() => {
    list<Partner>('partners').then(setPartners).catch(() => {});
  }, []);

  return (
    <aside className="flex w-64 flex-col border-r border-default bg-sidebar">
      <div className="flex items-center gap-2 border-b border-default px-4 py-5">
        <span className="text-lg text-brand-primary">⚡</span>
        <span className="text-base font-bold text-main">BorneMap</span>
      </div>

      <nav className="flex-1 space-y-1 px-3 py-4">
        {nav.map((item) => (
          <NavigationItem key={item.to} to={item.to} icon={item.icon} label={item.label} />
        ))}
      </nav>

      <div className="border-t border-default px-3 py-4">
        <div className="flex items-center justify-between">
          <span className="text-xs font-medium text-muted">
            {role === 'admin' ? 'Admin View' : 'Partner View'}
          </span>
          <button
            onClick={() => setRole(role === 'admin' ? 'partner' : 'admin')}
            className="rounded bg-neutral-100 px-2 py-1 text-xs font-medium text-muted hover:bg-neutral-200"
          >
            Switch
          </button>
        </div>

        {role === 'partner' && (
          <select
            value={selectedPartnerId || ''}
            onChange={(e) => setSelectedPartnerId(e.target.value || null)}
            className="mt-2 w-full rounded border border-default px-2 py-1 text-xs text-main"
          >
            <option value="">Select partner...</option>
            {partners.map((p) => (
              <option key={p.id} value={p.id}>{p.name}</option>
            ))}
          </select>
        )}

        <p className="mt-2 text-[10px] text-neutral-400 italic">
          Dev Only — removed in MVP-3
        </p>
      </div>
    </aside>
  );
}
