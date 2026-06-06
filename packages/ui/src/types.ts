export type ButtonVariant = 'primary' | 'secondary' | 'ghost' | 'danger'
export type ButtonSize = 'sm' | 'md' | 'lg'
export type ButtonState = 'default' | 'hover' | 'active' | 'disabled' | 'loading'

export type InputVariant = 'default' | 'error' | 'search'
export type InputSize = 'sm' | 'md' | 'lg'
export type InputState = 'default' | 'focused' | 'error' | 'disabled'

export type BadgeVariant = 'default' | 'success' | 'warning' | 'error' | 'info'

export type StatusBadgeVariant = 'available' | 'in-use' | 'maintenance' | 'offline'
export type StatusBadgeState = 'default' | 'animating'

export type ToastVariant = 'success' | 'error' | 'warning' | 'info'

export type ModalSize = 'sm' | 'md' | 'lg'

export type SkeletonType = 'block' | 'text' | 'circular'

export interface TableColumn {
  key: string
  label: string
  sortable?: boolean
  width?: string | number
}

export interface TrendData {
  value: number
  positive?: boolean
}

export interface DataCardAction {
  label: string
  onClick: () => void
}
