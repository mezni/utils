import { render, screen } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import { Badge } from './Badge'

describe('Badge', () => {
  it('renders children text', () => {
    render(<Badge>Active</Badge>)
    expect(screen.getByText('Active')).toBeInTheDocument()
  })

  it('renders with default variant', () => {
    const { container } = render(<Badge>Default</Badge>)
    expect(container.querySelector('span')).toHaveClass('badge-default')
  })

  it('renders all variants', () => {
    const variants = ['default', 'success', 'warning', 'error', 'info'] as const
    variants.forEach((variant) => {
      const { container } = render(<Badge variant={variant}>{variant}</Badge>)
      expect(container.querySelector('span')).toHaveClass(`badge-${variant}`)
    })
  })
})
