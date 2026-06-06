import React, { useEffect, useCallback } from 'react'
import { createPortal } from 'react-dom'
import type { ModalSize } from '../../types'
import { neutral700, neutral500, neutral200 } from '../../tokens/colors'
import { fontSizeXl, fontSizeMd, fontWeightSemibold } from '../../tokens/typography'
import { spacing1, spacing4, spacing6 } from '../../tokens/spacing'
import { radiusLg, radiusMd } from '../../tokens/radius'
import { shadowFloat } from '../../tokens/shadows'

interface ModalProps {
  size?: ModalSize
  title?: string
  isOpen: boolean
  onClose: () => void
  children: React.ReactNode
}

const sizeStyles: Record<ModalSize, React.CSSProperties> = {
  sm: { maxWidth: 320 },
  md: { maxWidth: 480 },
  lg: { maxWidth: 640 },
}

export function Modal({ size = 'md', title, isOpen, onClose, children }: ModalProps) {
  const handleKeyDown = useCallback(
    (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose()
    },
    [onClose],
  )

  useEffect(() => {
    if (isOpen) {
      document.addEventListener('keydown', handleKeyDown)
      return () => document.removeEventListener('keydown', handleKeyDown)
    }
  }, [isOpen, handleKeyDown])

  if (!isOpen) return null

  return createPortal(
    <div
      className="modal-overlay"
      onClick={(e) => { if (e.target === e.currentTarget) onClose() }}
      style={{
        position: 'fixed',
        inset: 0,
        backgroundColor: 'rgba(0,0,0,0.4)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        zIndex: 1000,
      }}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-label={title}
        className={`modal-content modal-${size}`}
        onClick={(e) => e.stopPropagation()}
        style={{
          backgroundColor: '#fff',
          borderRadius: radiusLg,
          boxShadow: '0 8px 24px rgba(0,0,0,0.15)',
          width: '100%',
          ...sizeStyles[size],
          padding: `${spacing6}px`,
          position: 'relative',
          animation: 'modal-fade-in 0.2s ease-out',
        }}
      >
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            marginBottom: spacing4,
          }}
        >
          {title && (
            <h2 style={{ margin: 0, fontSize: fontSizeXl, fontWeight: fontWeightSemibold, color: neutral700 }}>
              {title}
            </h2>
          )}
          <button
            aria-label="Close"
            onClick={onClose}
            style={{
              background: 'none',
              border: `1px solid ${neutral200}`,
              borderRadius: radiusMd,
              cursor: 'pointer',
              fontSize: 20,
              lineHeight: 1,
              padding: `${spacing1}px`,
              color: neutral500,
            }}
          >
            ×
          </button>
        </div>
        {children}
      </div>
    </div>,
    document.body,
  )
}
