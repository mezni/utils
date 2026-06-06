import React, { useState, useCallback } from 'react'
import type { InputVariant, InputSize } from '../../types'
import { neutral200, neutral500, error as errorColor, neutral100 } from '../../tokens/colors'
import { fontSizeSm, fontWeightRegular } from '../../tokens/typography'
import { spacing1, spacing2, spacing3, spacing4 } from '../../tokens/spacing'
import { radiusMd } from '../../tokens/radius'

interface InputProps {
  variant?: InputVariant
  size?: InputSize
  disabled?: boolean
  error?: string
  placeholder?: string
  value?: string
  type?: 'text' | 'password' | 'search'
  onChange?: (value: string) => void
  'aria-label'?: string
}

const variantStyles: Record<InputVariant, React.CSSProperties> = {
  default: {
    borderColor: neutral200,
  },
  error: {
    borderColor: errorColor,
  },
  search: {
    borderColor: neutral200,
    backgroundColor: neutral100,
  },
}

const sizeStyles: Record<InputSize, React.CSSProperties> = {
  sm: { padding: `${spacing1}px ${spacing2}px`, fontSize: fontSizeSm },
  md: { padding: `${spacing2}px ${spacing3}px`, fontSize: 14 },
  lg: { padding: `${spacing3}px ${spacing4}px`, fontSize: 16 },
}

export function Input({
  variant = 'default',
  size = 'md',
  disabled = false,
  error,
  placeholder,
  value,
  type = 'text',
  onChange,
  'aria-label': ariaLabel,
}: InputProps) {
  const [isFocused, setIsFocused] = useState(false)
  const activeVariant = error ? 'error' : variant
  const resolvedVariant = isFocused ? 'default' : activeVariant

  const handleChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      onChange?.(e.target.value)
    },
    [onChange],
  )

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: `${spacing1}px` }}>
      <input
        className={`input input-${activeVariant} input-${size}${isFocused ? ' input-focused' : ''}${disabled ? ' input-disabled' : ''}`}
        type={type}
        placeholder={placeholder}
        value={value}
        disabled={disabled}
        onChange={handleChange}
        onFocus={() => setIsFocused(true)}
        onBlur={() => setIsFocused(false)}
        aria-label={ariaLabel}
        aria-invalid={!!error}
        aria-describedby={error ? 'input-error' : undefined}
        dir="auto"
        style={{
          ...variantStyles[activeVariant],
          ...sizeStyles[size],
          borderRadius: radiusMd,
          color: neutral500,
          fontWeight: fontWeightRegular,
          width: '100%',
          outline: isFocused ? `2px solid ${errorColor}` : 'none',
          outlineOffset: '2px',
          backgroundColor: disabled ? neutral200 : 'white',
          cursor: disabled ? 'not-allowed' : 'text',
          transition: 'border-color 0.2s, outline 0.2s',
          boxSizing: 'border-box',
        }}
      />
      {error && (
        <span id="input-error" style={{ color: errorColor, fontSize: fontSizeSm }}>
          {error}
        </span>
      )}
    </div>
  )
}
