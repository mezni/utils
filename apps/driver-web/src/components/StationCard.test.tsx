import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import { FavoritesProvider } from '../hooks/useFavorites'
import StationCard from './StationCard'

function renderWithFavorites(ui: React.ReactElement) {
  return render(<FavoritesProvider>{ui}</FavoritesProvider>)
}

const mockStation = {
  id: 'STN-001',
  name: 'Station Test',
  address: '15 Avenue Test',
  distance: 2.5,
  chargerCount: 4,
  availableCount: 2,
  availability: 'available' as const,
  rating: 4.2,
  reviewCount: 10,
}

describe('StationCard', () => {
  it('renders station name', () => {
    renderWithFavorites(<StationCard station={mockStation} onClick={() => {}} />)
    expect(screen.getByText('Station Test')).toBeInTheDocument()
  })

  it('renders address', () => {
    renderWithFavorites(<StationCard station={mockStation} onClick={() => {}} />)
    expect(screen.getByText('15 Avenue Test')).toBeInTheDocument()
  })

  it('renders distance', () => {
    renderWithFavorites(<StationCard station={mockStation} onClick={() => {}} />)
    expect(screen.getByText('2.5 km')).toBeInTheDocument()
  })

  it('renders charger count', () => {
    renderWithFavorites(<StationCard station={mockStation} onClick={() => {}} />)
    expect(screen.getByText('2/4 Chargeurs')).toBeInTheDocument()
  })

  it('renders rating', () => {
    renderWithFavorites(<StationCard station={mockStation} onClick={() => {}} />)
    expect(screen.getByText('4.2 (10)')).toBeInTheDocument()
  })

  it('shows available badge when station is available', () => {
    renderWithFavorites(<StationCard station={mockStation} onClick={() => {}} />)
    expect(screen.getByText('Disponible')).toBeInTheDocument()
  })

  it('shows unavailable badge when station is unavailable', () => {
    renderWithFavorites(<StationCard station={{ ...mockStation, availability: 'unavailable' }} onClick={() => {}} />)
    expect(screen.getByText('Indisponible')).toBeInTheDocument()
  })

  it('calls onClick with station id on card click', () => {
    const onClick = vi.fn()
    renderWithFavorites(<StationCard station={mockStation} onClick={onClick} />)
    fireEvent.click(screen.getByText('Station Test').closest('[role="button"]')!)
    expect(onClick).toHaveBeenCalledWith('STN-001')
  })

  it('renders favorite heart button', () => {
    renderWithFavorites(<StationCard station={mockStation} onClick={() => {}} />)
    expect(screen.getByLabelText('Add to favorites')).toBeInTheDocument()
  })

  it('toggles favorite on heart click', () => {
    const onClick = vi.fn()
    renderWithFavorites(<StationCard station={mockStation} onClick={onClick} />)
    const heartBtn = screen.getByLabelText('Add to favorites')
    fireEvent.click(heartBtn)
    expect(screen.getByLabelText('Remove from favorites')).toBeInTheDocument()
  })

  it('has focus styling', () => {
    renderWithFavorites(<StationCard station={mockStation} onClick={() => {}} />)
    const btn = screen.getByText('Station Test').closest('[role="button"]')
    expect(btn).toHaveClass('focus:ring-brand-primary')
  })
})
