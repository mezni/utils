import { NavLink, useLocation } from 'react-router-dom'

interface NavigationItemProps {
  path: string
  icon: string
  label: string
  badge?: string
}

export const NavigationItem = ({ path, icon, label, badge }: NavigationItemProps) => {
  const location = useLocation()
  const isActive = location.pathname === path

  return (
    <NavLink
      to={path}
      className={`
        flex items-center gap-3 px-4 py-3 rounded-lg mb-1 transition-all duration-200
        ${isActive 
          ? 'bg-brand-sageLight text-brand-primary font-medium' 
          : 'text-text-muted hover:bg-surface-hover hover:text-text-primary'
        }
      `}
    >
      <span className="text-xl">{icon}</span>
      <span className="flex-1">{label}</span>
      {badge && (
        <span className="bg-brand-primary text-white text-xs px-2 py-1 rounded-full">
          {badge}
        </span>
      )}
    </NavLink>
  )
}