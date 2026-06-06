import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import ZoomControls from './ZoomControls'

describe('ZoomControls', () => {
  it('renders zoom in button', () => {
    render(<ZoomControls onZoomIn={() => {}} onZoomOut={() => {}} />)
    expect(screen.getByLabelText('Zoom in')).toBeInTheDocument()
  })

  it('renders zoom out button', () => {
    render(<ZoomControls onZoomIn={() => {}} onZoomOut={() => {}} />)
    expect(screen.getByLabelText('Zoom out')).toBeInTheDocument()
  })

  it('calls onZoomIn on + click', () => {
    const onZoomIn = vi.fn()
    render(<ZoomControls onZoomIn={onZoomIn} onZoomOut={() => {}} />)
    fireEvent.click(screen.getByLabelText('Zoom in'))
    expect(onZoomIn).toHaveBeenCalledTimes(1)
  })

  it('calls onZoomOut on - click', () => {
    const onZoomOut = vi.fn()
    render(<ZoomControls onZoomIn={() => {}} onZoomOut={onZoomOut} />)
    fireEvent.click(screen.getByLabelText('Zoom out'))
    expect(onZoomOut).toHaveBeenCalledTimes(1)
  })

  it('zoom in button has focus ring', () => {
    render(<ZoomControls onZoomIn={() => {}} onZoomOut={() => {}} />)
    const btn = screen.getByLabelText('Zoom in')
    expect(btn).toHaveClass('focus:ring-brand-primary')
  })

  it('zoom out button has focus ring', () => {
    render(<ZoomControls onZoomIn={() => {}} onZoomOut={() => {}} />)
    const btn = screen.getByLabelText('Zoom out')
    expect(btn).toHaveClass('focus:ring-brand-primary')
  })

  it('displays + and − symbols', () => {
    render(<ZoomControls onZoomIn={() => {}} onZoomOut={() => {}} />)
    expect(screen.getByText('+')).toBeInTheDocument()
    expect(screen.getByText('−')).toBeInTheDocument()
  })
})
