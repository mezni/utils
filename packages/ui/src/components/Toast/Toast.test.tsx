import { render, screen, fireEvent, act } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import { Toast } from './Toast'

describe('Toast', () => {
  it('renders title', () => {
    render(<Toast variant="success" title="Saved!" />)
    expect(screen.getByText('Saved!')).toBeInTheDocument()
  })

  it('renders message when provided', () => {
    render(<Toast variant="success" title="Saved!" message="Your changes are saved" />)
    expect(screen.getByText('Your changes are saved')).toBeInTheDocument()
  })

  it('renders all variants', () => {
    const variants = ['success', 'error', 'warning', 'info'] as const
    variants.forEach((variant) => {
      const { container } = render(<Toast variant={variant} title={variant} />)
      expect(container.querySelector('.toast')).toHaveClass(`toast-${variant}`)
    })
  })

  it('calls onClose when close button clicked', () => {
    const onClose = vi.fn()
    render(<Toast variant="info" title="Info" showCloseButton onClose={onClose} />)
    fireEvent.click(screen.getByLabelText('Close'))
    expect(onClose).toHaveBeenCalledTimes(1)
  })

  it('auto-dismisses after duration', () => {
    vi.useFakeTimers()
    const onClose = vi.fn()
    render(<Toast variant="info" title="Auto" duration={3000} onClose={onClose} />)
    act(() => { vi.advanceTimersByTime(3000) })
    expect(onClose).toHaveBeenCalledTimes(1)
    vi.useRealTimers()
  })

  it('has alert role', () => {
    render(<Toast variant="success" title="Done" />)
    expect(screen.getByRole('alert')).toBeInTheDocument()
  })
})
