import { render, screen } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import { FavoritesProvider } from '../hooks/useFavorites'
import StationDetailScreen from './StationDetailScreen'

function renderScreen(stationId = 'STN-001') {
  return render(
    <MemoryRouter initialEntries={[`/stations/${stationId}`]}>
      <FavoritesProvider>
        <Routes>
          <Route path="/stations/:id" element={<StationDetailScreen />} />
        </Routes>
      </FavoritesProvider>
    </MemoryRouter>,
  )
}

describe('StationDetailScreen', () => {
  it('renders station name', () => {
    renderScreen('STN-001')
    expect(screen.getByText('Station de recharge Ariana')).toBeInTheDocument()
  })

  it('renders station address', () => {
    renderScreen('STN-001')
    expect(screen.getByText('15 Avenue Habib Bourguiba, Ariana 2080')).toBeInTheDocument()
  })

  it('renders chargers section', () => {
    renderScreen('STN-001')
    expect(screen.getByText('Chargeurs (2/3 Disponible)')).toBeInTheDocument()
  })

  it('renders charger rows', () => {
    renderScreen('STN-001')
    expect(screen.getByText('Type 2')).toBeInTheDocument()
    expect(screen.getByText('CCS')).toBeInTheDocument()
    expect(screen.getByText('CHAdeMO')).toBeInTheDocument()
  })

  it('renders reviews section', () => {
    renderScreen('STN-001')
    expect(screen.getByText('Avis')).toBeInTheDocument()
  })

  it('renders review authors', () => {
    renderScreen('STN-001')
    expect(screen.getByText('Ahmed Ben Salem')).toBeInTheDocument()
    expect(screen.getByText('Sophie Martin')).toBeInTheDocument()
  })

  it('shows error state for unknown station', () => {
    renderScreen('STN-999')
    expect(screen.getByText('Une erreur est survenue')).toBeInTheDocument()
  })
})
