import { render, screen } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import { MemoryRouter } from 'react-router-dom'
import { FavoritesProvider } from '../hooks/useFavorites'
import FavoritesScreen from './FavoritesScreen'

function renderScreen() {
  return render(
    <MemoryRouter>
      <FavoritesProvider initialFavorites={['STN-001', 'STN-003', 'STN-011']}>
        <FavoritesScreen />
      </FavoritesProvider>
    </MemoryRouter>,
  )
}

describe('FavoritesScreen', () => {
  it('renders favorites title', () => {
    renderScreen()
    expect(screen.getByText('Favoris')).toBeInTheDocument()
  })

  it('renders favorite stations', () => {
    renderScreen()
    expect(screen.getByText('Station de recharge Ariana')).toBeInTheDocument()
    expect(screen.getByText('Bornes Lac Tunis')).toBeInTheDocument()
    expect(screen.getByText('Bornes Charguia')).toBeInTheDocument()
  })

  it('shows empty state when no favorites', () => {
    render(
      <MemoryRouter>
        <FavoritesProvider initialFavorites={[]}>
          <FavoritesScreen />
        </FavoritesProvider>
      </MemoryRouter>,
    )
    expect(screen.getByText('Aucune station en favori')).toBeInTheDocument()
  })
})
