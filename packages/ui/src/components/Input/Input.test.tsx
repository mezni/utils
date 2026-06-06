import { render, screen, fireEvent, act } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import { Input } from './Input'

describe('Input', () => {
  it('renders with placeholder', () => {
    render(<Input placeholder="Enter text" />)
    expect(screen.getByPlaceholderText('Enter text')).toBeInTheDocument()
  })

  it('renders all variants', () => {
    const variants = ['default', 'error', 'search'] as const
    variants.forEach((variant) => {
      const { container } = render(<Input variant={variant} />)
      expect(container.querySelector('input')).toHaveClass(`input-${variant}`)
    })
  })

  it('renders all sizes', () => {
    const sizes = ['sm', 'md', 'lg'] as const
    sizes.forEach((size) => {
      const { container } = render(<Input size={size} placeholder="test" />)
      expect(container.querySelector('input')).toHaveClass(`input-${size}`)
    })
  })

  it('shows error message', () => {
    render(<Input error="This field is required" />)
    expect(screen.getByText('This field is required')).toBeInTheDocument()
  })

  it('applies error state when error prop provided', () => {
    const { container } = render(<Input error="Error" />)
    expect(container.querySelector('input')).toHaveClass('input-error')
  })

  it('handles disabled state', () => {
    render(<Input disabled placeholder="disabled" />)
    expect(screen.getByPlaceholderText('disabled')).toBeDisabled()
  })

  it('calls onChange with value', () => {
    const onChange = vi.fn()
    render(<Input onChange={onChange} />)
    const input = screen.getByRole('textbox')
    fireEvent.change(input, { target: { value: 'hello' } })
    expect(onChange).toHaveBeenCalledWith('hello')
  })

  it('renders focused state', () => {
    const { container } = render(<Input placeholder="focus" />)
    const input = container.querySelector('input')!
    act(() => { input.focus() })
    expect(input).toHaveStyle('outline: 2px solid #ef4444')
  })

  it('has accessible label', () => {
    render(<Input aria-label="Search" />)
    expect(screen.getByLabelText('Search')).toBeInTheDocument()
  })
})
