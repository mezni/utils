import React from 'react'
import type { TrendData } from '../../types'
import { neutral700, neutral500, success, error as errorColor } from '../../tokens/colors'
import { fontSizeSm, fontSizeMd, fontSize2xl, fontWeightMedium, fontWeightSemibold, fontWeightBold } from '../../tokens/typography'
import { spacing1, spacing3, spacing4 } from '../../tokens/spacing'
import { radiusMd } from '../../tokens/radius'
import { shadowCard } from '../../tokens/shadows'

interface StatCardProps {
  label: string
  value: string | number
  trend?: TrendData
  icon?: React.ReactNode
}

export function StatCard({ label, value, trend, icon }: StatCardProps) {
  return (
    <div
      style={{
        backgroundColor: '#fff',
        borderRadius: radiusMd,
        boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
        padding: spacing4,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        gap: spacing3,
      }}
    >
      <div style={{ display: 'flex', flexDirection: 'column', gap: spacing1 }}>
        <span style={{ fontSize: fontSizeSm, fontWeight: fontWeightMedium, color: neutral500 }}>
          {label}
        </span>
        <span style={{ fontSize: fontSize2xl, fontWeight: fontWeightBold, color: neutral700 }}>
          {value}
        </span>
        {trend && (
          <span
            style={{
              fontSize: fontSizeSm,
              fontWeight: fontWeightSemibold,
              color: trend.positive ? success : errorColor,
            }}
          >
            {trend.positive ? '↑' : '↓'} {trend.value}%
          </span>
        )}
      </div>
      {icon && <div aria-hidden="true">{icon}</div>}
    </div>
  )
}
