import React from 'react'
import ReactDOM from 'react-dom/client'
import { RouterProvider } from 'react-router-dom'
import './i18n'
import './index.css'
import { router } from './App'
import { FavoritesProvider } from './hooks/useFavorites'
import { users } from './mocks/users'

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <FavoritesProvider initialFavorites={users[0]?.favoriteStationIds || []}>
      <RouterProvider router={router} />
    </FavoritesProvider>
  </React.StrictMode>,
)
