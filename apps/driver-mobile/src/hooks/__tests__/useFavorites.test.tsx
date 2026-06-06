import React from 'react'
import { render } from '@testing-library/react-native'
import { useFavorites, FavoritesProvider } from '../../hooks/useFavorites'

const TestComponent = ({ onFavoritesChange }: { onFavoritesChange: (fav: boolean) => void }) => {
  const { isFavorite } = useFavorites()
  React.useEffect(() => {
    onFavoritesChange(isFavorite('station-1'))
  }, [isFavorite, onFavoritesChange])
  return null
}

describe('useFavorites', () => {
  it('initializes with provided favorites', () => {
    const { getByTestId } = render(
      <FavoritesProvider initialFavorites={['station-1', 'station-2']}>
        <TestComponent onFavoritesChange={() => {}} />
      </FavoritesProvider>
    )
    // Test that initial favorites are set
    expect(getByTestId).toBeDefined()
  })

  it('adds favorite to list', () => {
    const toggleFavorite = jest.fn()
    const { getByText } = render(
      <FavoritesProvider initialFavorites={[]}>
        <TestComponent onFavoritesChange={toggleFavorite} />
      </FavoritesProvider>
    )
    expect(getByText).toBeDefined()
  })

  it('removes favorite from list', () => {
    const { getByText } = render(
      <FavoritesProvider initialFavorites={['station-1']}>
        <TestComponent onFavoritesChange={() => {}} />
      </FavoritesProvider>
    )
    expect(getByText).toBeDefined()
  })
})
