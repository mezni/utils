import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import { EmptyState } from './EmptyState'

describe('EmptyState', () => {
  it('renders title', () => {
    render(<EmptyState title="No results" />)
    expect(screen.getByText('No results')).toBeInTheDocument()
  })

  it('renders description when provided', () => {
    render(<EmptyState title="No results" description="Try again" />)
    expect(screen.getByText('Try again')).toBeInTheDocument()
  })

  it('renders icon when provided', () => {
    render(<EmptyState title="No results" icon={<span data-testid="icon" />} />)
    expect(screen.getByTestId('icon')).toBeInTheDocument()
  })

  it('renders action button when provided', () => {
    render(<EmptyState title="No results" action={{ label: 'Retry', onClick: () => {} }} />)
    expect(screen.getByText('Retry')).toBeInTheDocument()
  })

  it('calls action onClick when button clicked', () => {
    const onClick = vi.fn()
    render(<EmptyState title="No results" action={{ label: 'Retry', onClick }} />)
    fireEvent.click(screen.getByText('Retry'))
    expect(onClick).toHaveBeenCalledTimes(1)
  })
})
