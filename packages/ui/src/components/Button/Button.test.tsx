import { render, screen, fireEvent, act } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import { Button } from './Button'

describe('Button', () => {
  it('renders children text', () => {
    render(<Button>Click me</Button>)
    expect(screen.getByText('Click me')).toBeInTheDocument()
  })

  it('renders with primary variant by default', () => {
    const { container } = render(<Button>Primary</Button>)
    const btn = container.querySelector('button')
    expect(btn).toHaveClass('btn-primary')
  })

  it('renders all variants', () => {
    const variants = ['primary', 'secondary', 'ghost', 'danger'] as const
    variants.forEach((variant) => {
      const { container } = render(<Button variant={variant}>{variant}</Button>)
      expect(container.querySelector('button')).toHaveClass(`btn-${variant}`)
    })
  })

  it('renders all sizes', () => {
    const sizes = ['sm', 'md', 'lg'] as const
    sizes.forEach((size) => {
      const { container } = render(<Button size={size}>{size}</Button>)
      expect(container.querySelector('button')).toHaveClass(`btn-${size}`)
    })
  })

  it('renders in disabled state', () => {
    render(<Button disabled>Disabled</Button>)
    expect(screen.getByRole('button')).toBeDisabled()
  })

  it('renders in loading state', () => {
    render(<Button loading>Loading</Button>)
    const btn = screen.getByRole('button')
    expect(btn).toBeDisabled()
    expect(btn.querySelector('.btn-spinner')).toBeInTheDocument()
  })

  it('calls onClick when clicked', () => {
    const onClick = vi.fn()
    render(<Button onClick={onClick}>Click</Button>)
    fireEvent.click(screen.getByText('Click'))
    expect(onClick).toHaveBeenCalledTimes(1)
  })

  it('does not call onClick when disabled', () => {
    const onClick = vi.fn()
    render(<Button onClick={onClick} disabled>Click</Button>)
    fireEvent.click(screen.getByText('Click'))
    expect(onClick).not.toHaveBeenCalled()
  })

  it('is keyboard accessible', () => {
    const onClick = vi.fn()
    render(<Button onClick={onClick}>Enter</Button>)
    const btn = screen.getByRole('button')
    act(() => { btn.focus() })
    expect(document.activeElement).toBe(btn)
    fireEvent.keyDown(btn, { key: 'Enter' })
    expect(onClick).toHaveBeenCalled()
  })

  it('has visible focus indicator', () => {
    const { container } = render(<Button>Focus</Button>)
    const btn = container.querySelector('button')!
    act(() => { btn.focus() })
    expect(btn).toHaveStyle('outline: 2px solid #007943')
  })

  it('supports RTL dir attribute', () => {
    const { container } = render(<Button>RTL</Button>)
    const btn = container.querySelector('button')
    expect(btn).toHaveAttribute('dir', 'auto')
  })
})
