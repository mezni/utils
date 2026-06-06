import React from 'react'
import { neutral500, neutral200 } from '../../tokens/colors'
import { fontSizeLg, fontSizeMd, fontWeightSemibold, fontWeightRegular } from '../../tokens/typography'
import { spacing4, spacing6 } from '../../tokens/spacing'
import { Button } from '../Button/Button'

interface EmptyStateProps {
  icon?: React.ReactNode
  title: string
  description?: string
  action?: { label: string; onClick: () => void }
}

export function EmptyState({ icon, title, description, action }: EmptyStateProps) {
  return (
    <div
      role="status"
      style={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        gap: `${spacing4}px`,
        padding: `${spacing6}px`,
        textAlign: 'center',
      }}
    >
      {icon && <div aria-hidden="true">{icon}</div>}
      <h3
        style={{
          margin: 0,
          fontSize: fontSizeLg,
          fontWeight: fontWeightSemibold,
          color: neutral500,
        }}
      >
        {title}
      </h3>
      {description && (
        <p
          style={{
            margin: 0,
            fontSize: fontSizeMd,
            fontWeight: fontWeightRegular,
            color: neutral500,
          }}
        >
          {description}
        </p>
      )}
      {action && (
        <Button variant="ghost" onClick={action.onClick}>
          {action.label}
        </Button>
      )}
    </div>
  )
}
