import React, { useEffect } from 'react'
import type { ToastVariant } from '../../types'
import { success, warning, error as errorColor, brandPrimary } from '../../tokens/colors'
import { fontSizeSm, fontSizeMd, fontWeightMedium, fontWeightSemibold } from '../../tokens/typography'
import { spacing1, spacing2, spacing3, spacing4 } from '../../tokens/spacing'
import { radiusMd } from '../../tokens/radius'

interface ToastProps {
  variant?: ToastVariant
  title: string
  message?: string
  duration?: number
  onClose?: () => void
  showCloseButton?: boolean
}

const variantStyles: Record<ToastVariant, { bg: string; border: string }> = {
  success: { bg: '#d1fae5', border: success },
  error: { bg: '#fee2e2', border: errorColor },
  warning: { bg: '#fef3c7', border: warning },
  info: { bg: '#dbeafe', border: brandPrimary },
}

export function Toast({
  variant = 'info',
  title,
  message,
  duration = 5000,
  onClose,
  showCloseButton = true,
}: ToastProps) {
  useEffect(() => {
    if (duration > 0 && onClose) {
      const timer = setTimeout(onClose, duration)
      return () => clearTimeout(timer)
    }
  }, [duration, onClose])

  const styles = variantStyles[variant]

  return (
    <div
      role="alert"
      className={`toast toast-${variant}`}
      style={{
        display: 'flex',
        alignItems: 'flex-start',
        gap: `${spacing2}px`,
        padding: `${spacing3}px ${spacing4}px`,
        backgroundColor: styles.bg,
        borderLeft: `4px solid ${styles.border}`,
        borderRadius: radiusMd,
        boxShadow: '0 2px 8px rgba(0,0,0,0.1)',
        minWidth: 280,
        maxWidth: 400,
        animation: 'toast-slide-in 0.3s ease-out',
      }}
    >
      <div style={{ flex: 1 }}>
        <p style={{ margin: 0, fontSize: fontSizeMd, fontWeight: fontWeightSemibold }}>{title}</p>
        {message && (
          <p style={{ margin: `${spacing1}px 0 0`, fontSize: fontSizeSm, fontWeight: fontWeightMedium }}>{message}</p>
        )}
      </div>
      {showCloseButton && onClose && (
        <button
          aria-label="Close"
          onClick={onClose}
          style={{
            background: 'none',
            border: 'none',
            cursor: 'pointer',
            fontSize: 18,
            lineHeight: 1,
            padding: 0,
          }}
        >
          ×
        </button>
      )}
    </div>
  )
}
