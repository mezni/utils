interface Crumb {
  label: string;
  onClick?: () => void;
}

interface BreadcrumbNavProps {
  crumbs: Crumb[];
}

export function BreadcrumbNav({ crumbs }: BreadcrumbNavProps) {
  return (
    <nav className="flex items-center gap-1.5 text-xs mb-4">
      {crumbs.map((crumb, i) => (
        <span key={i} className="flex items-center gap-1.5">
          {i > 0 && <svg className="w-3 h-3 text-gray-700" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" /></svg>}
          {crumb.onClick ? (
            <button onClick={crumb.onClick} className="text-gray-500 hover:text-orange-400 transition-colors font-medium">
              {crumb.label}
            </button>
          ) : (
            <span className="text-gray-300 font-medium">{crumb.label}</span>
          )}
        </span>
      ))}
    </nav>
  );
}
