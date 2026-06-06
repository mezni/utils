import { render, screen } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import { MemoryRouter } from 'react-router-dom'
import { FavoritesProvider } from '../hooks/useFavorites'
import type { ReactNode } from 'react'
import HomeMapScreen from './HomeMapScreen'

function renderScreen() {
  return render(
    <MemoryRouter>
      <FavoritesProvider>{<HomeMapScreen />}</FavoritesProvider>
    </MemoryRouter>,
  )
}

describe('HomeMapScreen', () => {
  it('renders MobileTopBar with brand name', () => {
    renderScreen()
    expect(screen.getByText('BorneMap')).toBeInTheDocument()
  })

  it('renders search input', () => {
    renderScreen()
    expect(screen.getByPlaceholderText('Rechercher une station...')).toBeInTheDocument()
  })

  it('renders filter pills', () => {
    renderScreen()
    expect(screen.getByText('Type 2')).toBeInTheDocument()
  })

  it('renders station cards in sidebar', () => {
    renderScreen()
    expect(screen.getByText('Station de recharge Ariana')).toBeInTheDocument()
  })

  it('renders zoom controls on map', () => {
    renderScreen()
    expect(screen.getByLabelText('Zoom in')).toBeInTheDocument()
    expect(screen.getByLabelText('Zoom out')).toBeInTheDocument()
  })

  it('renders map area', () => {
    const { container } = renderScreen()
    const mapArea = container.querySelector('.bg-\\[\\#EAF0E6\\]')
    expect(mapArea).toBeInTheDocument()
  })
})
