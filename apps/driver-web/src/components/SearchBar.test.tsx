import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import SearchBar from './SearchBar'

describe('SearchBar', () => {
  it('renders with placeholder', () => {
    render(<SearchBar value="" onChange={() => {}} onSubmit={() => {}} />)
    expect(screen.getByPlaceholderText('Rechercher une station...')).toBeInTheDocument()
  })

  it('renders custom placeholder', () => {
    render(<SearchBar value="" onChange={() => {}} onSubmit={() => {}} placeholder="Custom" />)
    expect(screen.getByPlaceholderText('Custom')).toBeInTheDocument()
  })

  it('displays value', () => {
    render(<SearchBar value="Tunis" onChange={() => {}} onSubmit={() => {}} />)
    expect(screen.getByDisplayValue('Tunis')).toBeInTheDocument()
  })

  it('calls onChange on input change', () => {
    const onChange = vi.fn()
    render(<SearchBar value="" onChange={onChange} onSubmit={() => {}} />)
    fireEvent.change(screen.getByRole('textbox'), { target: { value: 'Ariana' } })
    expect(onChange).toHaveBeenCalledWith('Ariana')
  })

  it('calls onSubmit on Enter key', () => {
    const onSubmit = vi.fn()
    render(<SearchBar value="Tunis" onChange={() => {}} onSubmit={onSubmit} />)
    fireEvent.keyDown(screen.getByRole('textbox'), { key: 'Enter' })
    expect(onSubmit).toHaveBeenCalledWith('Tunis')
  })

  it('does not call onSubmit on non-Enter key', () => {
    const onSubmit = vi.fn()
    render(<SearchBar value="Tunis" onChange={() => {}} onSubmit={onSubmit} />)
    fireEvent.keyDown(screen.getByRole('textbox'), { key: 'Escape' })
    expect(onSubmit).not.toHaveBeenCalled()
  })

  it('has accessible label', () => {
    render(<SearchBar value="" onChange={() => {}} onSubmit={() => {}} />)
    expect(screen.getByLabelText('Search stations')).toBeInTheDocument()
  })

  it('supports autoFocus', () => {
    render(<SearchBar value="" onChange={() => {}} onSubmit={() => {}} autoFocus />)
    expect(document.activeElement).toBe(screen.getByRole('textbox'))
  })
})
