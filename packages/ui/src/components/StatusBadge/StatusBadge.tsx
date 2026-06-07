import React from 'react'
import type { StatusBadgeVariant, StatusBadgeState } from '../../types'
import { success, warning, error as errorColor, neutral400, bgSuccess, bgWarning, bgError, bgNeutral400, textSuccess, textWarning, textError, textNeutral400 } from '../../tokens/colors'
import { fontSizeSm, fontWeightMedium } from '../../tokens/typography'
import { spacing1, spacing2 } from '../../tokens/spacing'
import { radiusSm } from '../../tokens/radius'

interface StatusBadgeProps {
  variant: StatusBadgeVariant
  state?: StatusBadgeState
  showDot?: boolean
  children?: React.ReactNode
}

const dotColors: Record<StatusBadgeVariant, string> = {
  available: success,
  'in-use': warning,
  maintenance: errorColor,
  offline: neutral400,
}

const bgColors: Record<StatusBadgeVariant, string> = {
  available: bgSuccess,
  'in-use': bgWarning,
  maintenance: bgError,
  offline: bgNeutral400,
}

const textColors: Record<StatusBadgeVariant, string> = {
  available: textSuccess,
  'in-use': textWarning,
  maintenance: textError,
  offline: textNeutral400,
}

export function StatusBadge({
  variant,
  state = 'default',
  showDot = true,
  children,
}: StatusBadgeProps) {
  return (
    <span
      role="status"
      className={`status-badge status-${variant}${state === 'animating' ? ' status-animating' : ''}`}
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: `${spacing1}px`,
        padding: `${spacing1}px ${spacing2}px`,
        backgroundColor: bgColors[variant],
        color: textColors[variant],
        fontSize: fontSizeSm,
        fontWeight: fontWeightMedium,
        borderRadius: radiusSm,
        lineHeight: 1,
        transition: state === 'animating' ? 'opacity 0.5s ease-in-out' : undefined,
      }}
    >
      {showDot && (
        <span
          className="status-dot"
          style={{
            width: 8,
            height: 8,
            borderRadius: '50%',
            backgroundColor: dotColors[variant],
            display: 'inline-block',
            flexShrink: 0,
          }}
          aria-hidden="true"
        />
      )}
      {children}
    </span>
  )
}
