import { render, screen } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import { StatusBadge } from './StatusBadge'

describe('StatusBadge', () => {
  it('renders children text', () => {
    render(<StatusBadge variant="available">Available</StatusBadge>)
    expect(screen.getByText('Available')).toBeInTheDocument()
  })

  it('renders all variants', () => {
    const variants = ['available', 'in-use', 'maintenance', 'offline'] as const
    variants.forEach((variant) => {
      const { container } = render(<StatusBadge variant={variant}>{variant}</StatusBadge>)
      expect(container.querySelector('span')).toHaveClass(`status-${variant}`)
    })
  })

  it('shows dot when showDot is true', () => {
    const { container } = render(<StatusBadge variant="available" showDot>Available</StatusBadge>)
    expect(container.querySelector('.status-dot')).toBeInTheDocument()
  })

  it('hides dot when showDot is false', () => {
    const { container } = render(<StatusBadge variant="available" showDot={false}>Available</StatusBadge>)
    expect(container.querySelector('.status-dot')).not.toBeInTheDocument()
  })

  it('has accessible label', () => {
    render(<StatusBadge variant="available">Available</StatusBadge>)
    expect(screen.getByRole('status')).toBeInTheDocument()
  })

  it('includes non-color indicator (dot)', () => {
    const { container } = render(<StatusBadge variant="available" showDot>Available</StatusBadge>)
    const dot = container.querySelector('.status-dot')
    expect(dot).toBeInTheDocument()
  })

  it('applies animating state', () => {
    const { container } = render(<StatusBadge variant="in-use" state="animating">In use</StatusBadge>)
    expect(container.querySelector('span')).toHaveClass('status-animating')
  })
})
