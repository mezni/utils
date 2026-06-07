import { useState } from 'react'
import { useTranslation } from 'react-i18next'

interface ColumnDef {
  key: string
  label: string
}

interface DataTableProps {
  columns: ColumnDef[]
  data: Record<string, any>[]
}

export const DataTable = ({ columns, data }: DataTableProps) => {
  const { t } = useTranslation()
  const [sortKey, setSortKey] = useState<string | null>(null)
  const [sortAsc, setSortAsc] = useState(true)

  const sortedData = [...data].sort((a, b) => {
    if (!sortKey) return 0
    const aVal = a[sortKey]
    const bVal = b[sortKey]
    
    if (aVal < bVal) return sortAsc ? -1 : 1
    if (aVal > bVal) return sortAsc ? 1 : -1
    return 0
  })

  const handleSort = (key: string) => {
    if (sortKey === key) {
      setSortAsc(!sortAsc)
    } else {
      setSortKey(key)
      setSortAsc(true)
    }
  }

  if (data.length === 0) {
    return (
      <div className="bg-surface-panel rounded-lg p-8 text-center text-text-muted">
        {t('common.noData')}
      </div>
    )
  }

  return (
    <div className="bg-surface-panel rounded-lg overflow-hidden border border-border-default">
      <table className="w-full">
        <thead>
          <tr className="bg-surface-hover">
            {columns.map((col) => (
              <th
                key={col.key}
                onClick={() => handleSort(col.key)}
                onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); handleSort(col.key) } }}
                tabIndex={0}
                role="button"
                aria-label={`Sort by ${col.label}`}
                className="px-6 py-3 text-left text-sm font-medium text-text-primary cursor-pointer hover:bg-surface-hover select-none focus:outline-none focus:ring-2 focus:ring-brand-primary focus:ring-inset"
              >
                <div className="flex items-center gap-2">
                  {col.label}
                  {sortKey === col.key && (sortAsc ? '↑' : '↓')}
                </div>
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sortedData.map((row, idx) => (
            <tr key={idx} className="border-t border-border-default hover:bg-surface-hover">
              {columns.map((col) => (
                <td key={col.key} className="px-6 py-4 text-sm text-text-primary">
                  {col.key === 'actions' ? (
                    <div className="flex gap-2">
                      <button className="text-brand-primary hover:underline focus:outline-none focus:ring-2 focus:ring-brand-primary rounded">{t('table.edit')}</button>
                      {Array.isArray(row[col.key]) && row[col.key].includes('manage') && (
                        <button className="text-brand-primary hover:underline focus:outline-none focus:ring-2 focus:ring-brand-primary rounded">{t('table.manage')}</button>
                      )}
                    </div>
                  ) : (
                    row[col.key]
                  )}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}