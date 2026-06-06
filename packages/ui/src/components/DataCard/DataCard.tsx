import React from 'react'
import type { DataCardAction } from '../../types'
import { neutral700 } from '../../tokens/colors'
import { fontSizeMd, fontWeightSemibold } from '../../tokens/typography'
import { spacing3, spacing4 } from '../../tokens/spacing'
import { radiusMd } from '../../tokens/radius'
import { Button } from '../Button/Button'

interface DataCardProps {
  title?: string
  action?: DataCardAction
  children: React.ReactNode
}

export function DataCard({ title, action, children }: DataCardProps) {
  return (
    <div
      style={{
        backgroundColor: '#fff',
        borderRadius: radiusMd,
        boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
      }}
    >
      {(title || action) && (
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            padding: `${spacing3}px ${spacing4}px`,
            borderBottom: '1px solid #e2e8f0',
          }}
        >
          {title && (
            <h3 style={{ margin: 0, fontSize: fontSizeMd, fontWeight: fontWeightSemibold, color: neutral700 }}>
              {title}
            </h3>
          )}
          {action && (
            <Button variant="ghost" size="sm" onClick={action.onClick}>
              {action.label}
            </Button>
          )}
        </div>
      )}
      <div style={{ padding: spacing4 }}>{children}</div>
    </div>
  )
}
