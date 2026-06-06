import React from 'react'
import type { BadgeVariant } from '../../types'
import { brandPrimary, success, warning, error as errorColor, neutral500, neutral100 } from '../../tokens/colors'
import { fontSizeSm, fontWeightMedium } from '../../tokens/typography'
import { spacing1, spacing2 } from '../../tokens/spacing'
import { radiusSm } from '../../tokens/radius'

interface BadgeProps {
  variant?: BadgeVariant
  children: React.ReactNode
}

const variantStyles: Record<BadgeVariant, React.CSSProperties> = {
  default: {
    backgroundColor: neutral100,
    color: neutral500,
  },
  success: {
    backgroundColor: '#d1fae5',
    color: '#065f46',
  },
  warning: {
    backgroundColor: '#fef3c7',
    color: '#92400e',
  },
  error: {
    backgroundColor: '#fee2e2',
    color: '#991b1b',
  },
  info: {
    backgroundColor: '#dbeafe',
    color: '#1e40af',
  },
}

export function Badge({ variant = 'default', children }: BadgeProps) {
  return (
    <span
      className={`badge badge-${variant}`}
      style={{
        ...variantStyles[variant],
        display: 'inline-flex',
        alignItems: 'center',
        padding: `${spacing1}px ${spacing2}px`,
        fontSize: fontSizeSm,
        fontWeight: fontWeightMedium,
        borderRadius: radiusSm,
        lineHeight: 1,
      }}
    >
      {children}
    </span>
  )
}
