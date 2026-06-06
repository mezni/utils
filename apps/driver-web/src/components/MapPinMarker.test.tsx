import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import MapPinMarker from './MapPinMarker'

describe('MapPinMarker', () => {
  const baseProps = {
    stationName: 'Station Test',
    hasAvailable: true,
    onClick: vi.fn(),
    position: { top: '50%', left: '30%' },
  }

  it('renders at correct position', () => {
    const { container } = render(<MapPinMarker {...baseProps} state="default" />)
    const btn = container.querySelector('button')
    expect(btn).toHaveStyle({ top: '50%', left: '30%' })
  })

  it('has accessible label with available status', () => {
    render(<MapPinMarker {...baseProps} state="default" />)
    expect(screen.getByLabelText('Station Test - available')).toBeInTheDocument()
  })

  it('has accessible label with unavailable status', () => {
    render(<MapPinMarker {...baseProps} hasAvailable={false} state="default" />)
    expect(screen.getByLabelText('Station Test - unavailable')).toBeInTheDocument()
  })

  it('applies success color when default and available', () => {
    const { container } = render(<MapPinMarker {...baseProps} state="default" />)
    const btn = container.querySelector('button')
    expect(btn).toHaveClass('bg-semantic-success')
  })

  it('applies neutral color when default and unavailable', () => {
    const { container } = render(<MapPinMarker {...baseProps} hasAvailable={false} state="default" />)
    const btn = container.querySelector('button')
    expect(btn).toHaveClass('bg-neutral-400')
  })

  it('applies brand color when selected', () => {
    const { container } = render(<MapPinMarker {...baseProps} state="selected" />)
    const btn = container.querySelector('button')
    expect(btn).toHaveClass('bg-brand-primary')
  })

  it('applies neutral color on unavailable state', () => {
    const { container } = render(<MapPinMarker {...baseProps} hasAvailable={false} state="unavailable" />)
    const btn = container.querySelector('button')
    expect(btn).toHaveClass('bg-neutral-400')
  })

  it('calls onClick when clicked', () => {
    const onClick = vi.fn()
    render(<MapPinMarker {...baseProps} state="default" onClick={onClick} />)
    fireEvent.click(screen.getByLabelText('Station Test - available'))
    expect(onClick).toHaveBeenCalledTimes(1)
  })

  it('has hover scale effect', () => {
    const { container } = render(<MapPinMarker {...baseProps} state="default" />)
    const btn = container.querySelector('button')
    expect(btn).toHaveClass('hover:scale-125')
  })
})
