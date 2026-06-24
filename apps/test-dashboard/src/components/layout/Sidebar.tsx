interface NavItem {
  id: string;
  label: string;
  count?: number;
  icon: React.ReactNode;
  active?: boolean;
  onClick: () => void;
}

interface SidebarProps {
  items: NavItem[];
  title?: string;
}

export function Sidebar({ items, title }: SidebarProps) {
  return (
    <div className="flex flex-col h-full">
      {title && (
        <div className="px-4 py-3 border-b border-gray-800">
          <p className="text-xs font-semibold text-gray-600 uppercase tracking-widest">{title}</p>
        </div>
      )}
      <nav className="flex-1 p-2 space-y-0.5 overflow-y-auto">
        {items.map((item) => (
          <button
            key={item.id}
            onClick={item.onClick}
            className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-all duration-150 relative group
              ${item.active
                ? 'text-orange-400 bg-orange-500/10 before:absolute before:left-0 before:top-1/4 before:h-1/2 before:w-0.5 before:bg-orange-400 before:rounded-full before:shadow-[0_0_8px_rgba(249,115,22,0.5)]'
                : 'text-gray-500 hover:text-gray-300 hover:bg-gray-800/50'}`}
          >
            <span className={`shrink-0 ${item.active ? 'text-orange-400' : 'text-gray-600 group-hover:text-gray-400'}`}>
              {item.icon}
            </span>
            <span className="truncate">{item.label}</span>
            {item.count !== undefined && (
              <span className={`ml-auto text-xs font-mono tabular-nums px-1.5 py-0.5 rounded
                ${item.active ? 'bg-orange-500/20 text-orange-400' : 'bg-gray-800 text-gray-500'}`}
              >
                {item.count}
              </span>
            )}
          </button>
        ))}
      </nav>
    </div>
  );
}
