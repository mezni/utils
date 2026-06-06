import { render, screen } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import { StatCard } from './StatCard'

describe('StatCard', () => {
  it('renders label', () => {
    render(<StatCard label="Total" value="100" />)
    expect(screen.getByText('Total')).toBeInTheDocument()
  })

  it('renders value', () => {
    render(<StatCard label="Total" value="100" />)
    expect(screen.getByText('100')).toBeInTheDocument()
  })

  it('renders positive trend', () => {
    render(<StatCard label="Total" value="100" trend={{ value: 12, positive: true }} />)
    expect(screen.getByText((content) => content.includes('12%'))).toBeInTheDocument()
  })

  it('renders negative trend', () => {
    render(<StatCard label="Total" value="100" trend={{ value: 5, positive: false }} />)
    expect(screen.getByText((content) => content.includes('5%'))).toBeInTheDocument()
  })

  it('renders icon when provided', () => {
    render(<StatCard label="Total" value="100" icon={<span data-testid="icon" />} />)
    expect(screen.getByTestId('icon')).toBeInTheDocument()
  })
})
