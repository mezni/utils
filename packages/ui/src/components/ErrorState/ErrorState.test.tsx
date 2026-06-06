import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import { ErrorState } from './ErrorState'

describe('ErrorState', () => {
  it('renders title', () => {
    render(<ErrorState title="Something went wrong" />)
    expect(screen.getByText('Something went wrong')).toBeInTheDocument()
  })

  it('renders description when provided', () => {
    render(<ErrorState title="Error" description="Try again later" />)
    expect(screen.getByText('Try again later')).toBeInTheDocument()
  })

  it('renders retry button when provided', () => {
    const retry = vi.fn()
    render(<ErrorState title="Error" retry={retry} />)
    expect(screen.getByText('Retry')).toBeInTheDocument()
  })

  it('calls retry on button click', () => {
    const retry = vi.fn()
    render(<ErrorState title="Error" retry={retry} />)
    fireEvent.click(screen.getByText('Retry'))
    expect(retry).toHaveBeenCalledTimes(1)
  })

  it('has alert role for screen readers', () => {
    render(<ErrorState title="Error" />)
    expect(screen.getByRole('alert')).toBeInTheDocument()
  })
})
