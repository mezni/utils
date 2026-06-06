import { render } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import { Skeleton } from './Skeleton'

describe('Skeleton', () => {
  it('renders block type', () => {
    const { container } = render(<Skeleton type="block" width={200} height={40} />)
    const el = container.querySelector('.skeleton')
    expect(el).toBeInTheDocument()
    expect(el).toHaveClass('skeleton-block')
  })

  it('renders text type', () => {
    const { container } = render(<Skeleton type="text" width={300} />)
    const el = container.querySelector('.skeleton')
    expect(el).toBeInTheDocument()
    expect(el).toHaveClass('skeleton-text')
  })

  it('renders circular type', () => {
    const { container } = render(<Skeleton type="circular" width={40} height={40} />)
    const el = container.querySelector('.skeleton')
    expect(el).toBeInTheDocument()
    expect(el).toHaveClass('skeleton-circular')
  })

  it('applies width and height', () => {
    const { container } = render(<Skeleton type="block" width={200} height={40} />)
    const el = container.querySelector('.skeleton') as HTMLElement
    expect(el.style.width).toBe('200px')
    expect(el.style.height).toBe('40px')
  })

  it('shows animation by default', () => {
    const { container } = render(<Skeleton type="text" width={100} />)
    expect(container.querySelector('.skeleton-animated')).toBeInTheDocument()
  })

  it('hides animation when animated is false', () => {
    const { container } = render(<Skeleton type="text" width={100} animated={false} />)
    expect(container.querySelector('.skeleton-animated')).not.toBeInTheDocument()
  })

  it('sets aria-busy for accessibility', () => {
    const { container } = render(<Skeleton type="text" width={100} />)
    expect(container.querySelector('.skeleton')).toHaveAttribute('aria-busy', 'true')
  })
})
