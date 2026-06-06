import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import { DataCard } from './DataCard'

describe('DataCard', () => {
  it('renders title when provided', () => {
    render(<DataCard title="Details"><p>Content</p></DataCard>)
    expect(screen.getByText('Details')).toBeInTheDocument()
  })

  it('renders children', () => {
    render(<DataCard><p>Content</p></DataCard>)
    expect(screen.getByText('Content')).toBeInTheDocument()
  })

  it('renders action button', () => {
    render(<DataCard title="Details" action={{ label: 'Edit', onClick: () => {} }}><p>Content</p></DataCard>)
    expect(screen.getByText('Edit')).toBeInTheDocument()
  })

  it('calls action onClick', () => {
    const onClick = vi.fn()
    render(<DataCard title="Details" action={{ label: 'Edit', onClick }}><p>Content</p></DataCard>)
    fireEvent.click(screen.getByText('Edit'))
    expect(onClick).toHaveBeenCalledTimes(1)
  })
})
