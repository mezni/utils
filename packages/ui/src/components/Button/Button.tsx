import React, { useState, useCallback, type KeyboardEvent } from 'react'
import type { ButtonVariant, ButtonSize, ButtonState } from '../../types'
import { brandPrimary, brandDark, neutral200, neutral500 } from '../../tokens/colors'
import { fontWeightMedium, fontWeightSemibold } from '../../tokens/typography'
import { spacing1, spacing2, spacing3, spacing4 } from '../../tokens/spacing'
import { radiusMd } from '../../tokens/radius'

interface ButtonProps {
  variant?: ButtonVariant
  size?: ButtonSize
  state?: ButtonState
  disabled?: boolean
  loading?: boolean
  children: React.ReactNode
  onClick?: () => void
}

const variantStyles: Record<ButtonVariant, React.CSSProperties> = {
  primary: {
    backgroundColor: brandPrimary,
    color: '#fff',
    border: `1px solid ${brandPrimary}`,
  },
  secondary: {
    backgroundColor: 'transparent',
    color: brandDark,
    border: `1px solid ${neutral200}`,
  },
  ghost: {
    backgroundColor: 'transparent',
    color: brandDark,
    border: '1px solid transparent',
  },
  danger: {
    backgroundColor: '#ef4444',
    color: '#fff',
    border: '1px solid #ef4444',
  },
}

const sizeStyles: Record<ButtonSize, React.CSSProperties> = {
  sm: {
    padding: `${spacing1}px ${spacing2}px`,
    fontSize: 12,
    fontWeight: fontWeightMedium,
  },
  md: {
    padding: `${spacing2}px ${spacing3}px`,
    fontSize: 14,
    fontWeight: fontWeightMedium,
  },
  lg: {
    padding: `${spacing3}px ${spacing4}px`,
    fontSize: 16,
    fontWeight: fontWeightSemibold,
  },
}

export function Button({
  variant = 'primary',
  size = 'md',
  state,
  disabled = false,
  loading = false,
  children,
  onClick,
}: ButtonProps) {
  const [isFocused, setIsFocused] = useState(false)
  const isDisabled = disabled || loading || state === 'disabled'

  const handleClick = useCallback(() => {
    if (!isDisabled && onClick) onClick()
  }, [isDisabled, onClick])

  const handleKeyDown = useCallback(
    (e: KeyboardEvent<HTMLButtonElement>) => {
      if ((e.key === 'Enter' || e.key === ' ') && !isDisabled && onClick) {
        e.preventDefault()
        onClick()
      }
    },
    [isDisabled, onClick],
  )

  const variantKey = variant === 'danger' ? 'danger' : variant === 'secondary' ? 'secondary' : variant === 'ghost' ? 'ghost' : 'primary'

  return (
    <button
      className={`btn btn-${variant} btn-${size}${isFocused ? ' btn-focused' : ''}${isDisabled ? ' btn-disabled' : ''}${loading ? ' btn-loading' : ''}`}
      style={{
        ...variantStyles[variantKey],
        ...sizeStyles[size],
        borderRadius: radiusMd,
        cursor: isDisabled ? 'not-allowed' : 'pointer',
        opacity: isDisabled ? 0.5 : 1,
        display: 'inline-flex',
        alignItems: 'center',
        justifyContent: 'center',
        gap: `${spacing2}px`,
        transition: 'opacity 0.2s, background-color 0.2s',
        outline: isFocused ? `2px solid ${brandPrimary}` : 'none',
        outlineOffset: '2px',
      }}
      onClick={handleClick}
      onKeyDown={handleKeyDown}
      onFocus={() => setIsFocused(true)}
      onBlur={() => setIsFocused(false)}
      disabled={isDisabled}
      aria-disabled={isDisabled}
      aria-busy={loading}
      dir="auto"
    >
      {loading && <span className="btn-spinner" style={{ width: 14, height: 14, border: '2px solid rgba(255,255,255,0.3)', borderTopColor: '#fff', borderRadius: '50%', animation: 'btn-spin 0.6s linear infinite' }} />}
      {children}
    </button>
  )
}
