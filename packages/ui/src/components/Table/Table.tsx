import React from 'react'
import type { TableColumn } from '../../types'
import { neutral700, neutral500, neutral200, neutral100 } from '../../tokens/colors'
import { fontSizeSm, fontSizeMd, fontWeightMedium, fontWeightSemibold } from '../../tokens/typography'
import { spacing1, spacing2, spacing3, spacing4 } from '../../tokens/spacing'

interface TableProps {
  columns: TableColumn[]
  data: Record<string, any>[]
  onRowAction?: (action: string, rowData: any) => void
  rowActions?: Array<{ label: string; icon: React.ReactNode }>
}

export function Table({ columns, data, onRowAction, rowActions }: TableProps) {
  if (data.length === 0) {
    return (
      <div style={{ textAlign: 'center', padding: spacing4, color: neutral500 }}>
        No data
      </div>
    )
  }

  return (
    <div style={{ overflowX: 'auto' }}>
      <table
        role="table"
        style={{
          width: '100%',
          borderCollapse: 'collapse',
          fontSize: fontSizeMd,
        }}
      >
        <thead>
          <tr style={{ backgroundColor: neutral100 }}>
            {columns.map((col) => (
              <th
                key={col.key}
                scope="col"
                aria-sort={col.sortable ? 'none' : undefined}
                style={{
                  padding: spacing3,
                  textAlign: 'left',
                  fontWeight: fontWeightSemibold,
                  color: neutral700,
                  borderBottom: `2px solid ${neutral200}`,
                  whiteSpace: 'nowrap',
                  width: col.width,
                }}
              >
                {col.label}
              </th>
            ))}
            {rowActions && rowActions.length > 0 && (
              <th style={{ width: 60, borderBottom: `2px solid ${neutral200}` }} />
            )}
          </tr>
        </thead>
        <tbody>
          {data.map((row, rowIndex) => (
            <tr
              key={rowIndex}
              style={{
                borderBottom: `1px solid ${neutral200}`,
                transition: 'background-color 0.15s',
              }}
              onMouseEnter={(e) => (e.currentTarget.style.backgroundColor = neutral100)}
              onMouseLeave={(e) => (e.currentTarget.style.backgroundColor = 'transparent')}
            >
              {columns.map((col) => (
                <td
                  key={col.key}
                  style={{
                    padding: spacing3,
                    color: neutral500,
                    fontWeight: fontWeightMedium,
                    fontSize: fontSizeSm,
                  }}
                >
                  {row[col.key]}
                </td>
              ))}
              {rowActions && rowActions.length > 0 && (
                <td style={{ padding: spacing2 }}>
                  <div style={{ display: 'flex', gap: spacing2 }}>
                    {rowActions.map((action) => (
                      <button
                        key={action.label}
                        aria-label={action.label}
                        onClick={() => onRowAction?.(action.label, row)}
                        style={{
                          background: 'none',
                          border: 'none',
                          cursor: 'pointer',
                          padding: spacing1,
                          color: neutral500,
                          borderRadius: 4,
                        }}
                      >
                        {action.icon}
                      </button>
                    ))}
                  </div>
                </td>
              )}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
