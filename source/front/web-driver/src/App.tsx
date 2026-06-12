import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { ThemeProvider } from '@bornemap/ui'
import { BrowserRouter, Routes, Route } from 'react-router-dom'

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 5 * 60 * 1000,
      refetchOnWindowFocus: false,
      retry: 2,
    },
  },
})

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <ThemeProvider>
        <BrowserRouter>
          <Routes>
            <Route path="/" element={<div className="p-8">Map Screen</div>} />
            <Route path="/stations" element={<div className="p-8">Station List</div>} />
            <Route path="/stations/:id" element={<div className="p-8">Station Detail</div>} />
          </Routes>
        </BrowserRouter>
      </ThemeProvider>
    </QueryClientProvider>
  )
}

export default App
