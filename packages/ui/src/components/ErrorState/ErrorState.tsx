import React from 'react'
import { error as errorColor, neutral500 } from '../../tokens/colors'
import { fontSizeLg, fontSizeMd, fontWeightSemibold, fontWeightRegular } from '../../tokens/typography'
import { spacing4, spacing6 } from '../../tokens/spacing'
import { Button } from '../Button/Button'

interface ErrorStateProps {
  icon?: React.ReactNode
  title: string
  description?: string
  retry?: () => void
}

export function ErrorState({ icon, title, description, retry }: ErrorStateProps) {
  return (
    <div
      role="alert"
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
          color: errorColor,
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
      {retry && (
        <Button variant="danger" onClick={retry}>
          Retry
        </Button>
      )}
    </div>
  )
}
