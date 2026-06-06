import { render, screen } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import { MemoryRouter } from 'react-router-dom'
import { FavoritesProvider } from '../hooks/useFavorites'
import SearchResultsScreen from './SearchResultsScreen'

function renderScreen() {
  return render(
    <MemoryRouter initialEntries={['/search?q=Tunis']}>
      <FavoritesProvider>
        <SearchResultsScreen />
      </FavoritesProvider>
    </MemoryRouter>,
  )
}

describe('SearchResultsScreen', () => {
  it('renders search input with autoFocus', () => {
    renderScreen()
    expect(screen.getByPlaceholderText('Rechercher une station...')).toBeInTheDocument()
  })

  it('renders filter pills', () => {
    renderScreen()
    expect(screen.getAllByText('Tous')).toHaveLength(2)
  })

  it('renders search title', () => {
    renderScreen()
    expect(screen.getByText('Résultats de recherche')).toBeInTheDocument()
  })

  it('renders station cards for search query', () => {
    renderScreen()
    expect(screen.getByText('محطة شحن تونس المركزية')).toBeInTheDocument()
  })

  it('shows empty state when no results', () => {
    render(
      <MemoryRouter initialEntries={['/search?q=ZZZZZZNONEXISTENT']}>
        <FavoritesProvider>
          <SearchResultsScreen />
        </FavoritesProvider>
      </MemoryRouter>,
    )
    expect(screen.getByText('Aucun résultat trouvé')).toBeInTheDocument()
  })
})
