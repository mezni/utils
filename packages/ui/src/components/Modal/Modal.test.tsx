import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import { Modal } from './Modal'

describe('Modal', () => {
  it('renders when isOpen is true', () => {
    render(<Modal isOpen onClose={() => {}} title="Confirm"><p>Content</p></Modal>)
    expect(screen.getByText('Confirm')).toBeInTheDocument()
  })

  it('does not render when isOpen is false', () => {
    render(<Modal isOpen={false} onClose={() => {}} title="Hidden"><p>Content</p></Modal>)
    expect(screen.queryByText('Hidden')).not.toBeInTheDocument()
  })

  it('renders children', () => {
    render(<Modal isOpen onClose={() => {}}><p>Content</p></Modal>)
    expect(screen.getByText('Content')).toBeInTheDocument()
  })

  it('calls onClose when overlay clicked', () => {
    const onClose = vi.fn()
    render(<Modal isOpen onClose={onClose}><p>Content</p></Modal>)
    const overlay = document.querySelector('.modal-overlay')!
    fireEvent.click(overlay)
    expect(onClose).toHaveBeenCalledTimes(1)
  })

  it('calls onClose on Escape key', () => {
    const onClose = vi.fn()
    render(<Modal isOpen onClose={onClose}><p>Content</p></Modal>)
    fireEvent.keyDown(document, { key: 'Escape' })
    expect(onClose).toHaveBeenCalledTimes(1)
  })

  it('renders all sizes', () => {
    const sizes = ['sm', 'md', 'lg'] as const
    sizes.forEach((size) => {
      const { unmount } = render(<Modal isOpen onClose={() => {}} size={size}><p>{size}</p></Modal>)
      const content = document.querySelector('.modal-content')
      expect(content).toHaveClass(`modal-${size}`)
      unmount()
    })
  })
})
