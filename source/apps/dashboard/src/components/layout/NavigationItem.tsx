import { NavLink } from 'react-router-dom';

interface NavigationItemProps {
  to: string;
  icon: React.ReactNode;
  label: string;
}

export function NavigationItem({ to, icon, label }: NavigationItemProps) {
  return (
    <NavLink
      to={to}
      end
      className={({ isActive }) =>
        `flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors ${
          isActive
            ? 'bg-brand-sageLight text-brand-primary'
            : 'text-muted hover:bg-neutral-100 hover:text-main'
        }`
      }
    >
      {icon}
      <span>{label}</span>
    </NavLink>
  );
}
