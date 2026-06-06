import React, { createContext, useContext, useState, useCallback } from 'react'

interface FavoritesContextType {
  favoriteStationIds: string[]
  isFavorite: (id: string) => boolean
  toggleFavorite: (id: string) => void
}

const FavoritesContext = createContext<FavoritesContextType>({
  favoriteStationIds: [],
  isFavorite: () => false,
  toggleFavorite: () => {},
})

export function FavoritesProvider({ children }: { children: React.ReactNode }) {
  const [favoriteStationIds, setFavoriteStationIds] = useState<string[]>([])

  const isFavorite = useCallback(
    (id: string) => favoriteStationIds.includes(id),
    [favoriteStationIds],
  )

  const toggleFavorite = useCallback((id: string) => {
    setFavoriteStationIds((prev) =>
      prev.includes(id)
        ? prev.filter((fid) => fid !== id)
        : [...prev, id],
    )
  }, [])

  return (
    <FavoritesContext.Provider value={{ favoriteStationIds, isFavorite, toggleFavorite }}>
      {children}
    </FavoritesContext.Provider>
  )
}

export function useFavorites() {
  return useContext(FavoritesContext)
}
