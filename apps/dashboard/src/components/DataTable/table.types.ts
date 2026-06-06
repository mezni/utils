export interface Column<T> {
  key: string
  label: string
  sortable?: boolean
  render?: (row: T) => React.ReactNode
}

export interface DataTableProps<T> {
  columns: Column<T>[]
  data: T[]
  sortable?: boolean
  onSort?: (key: string, direction: 'asc' | 'desc') => void
  currentPage?: number
  pageSize?: number
  total?: number
  onPageChange?: (page: number) => void
  actions?: (row: T) => React.ReactNode
}

export interface SortState {
  key: string
  direction: 'asc' | 'desc'
}