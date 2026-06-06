import { useSearchParams } from 'react-router-dom'
import { useTranslation } from 'react-i18next'
import { useNavigate } from 'react-router-dom'
import MobileTopBar from '../components/MobileTopBar'
import SearchBar from '../components/SearchBar'
import FilterPills from '../components/FilterPills'
import StationCard from '../components/StationCard'
import { useMockFilter } from '../hooks/useMockFilter'
import { useState, useEffect } from 'react'

export default function SearchResultsScreen() {
  const { t } = useTranslation()
  const navigate = useNavigate()
  const [searchParams] = useSearchParams()
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const { filter, setChargerType, setAvailability, setSearchQuery, filteredStations } = useMockFilter()

  useEffect(() => {
    const q = searchParams.get('q') || ''
    setSearchQuery(q)
  }, [searchParams, setSearchQuery])

  return (
    <div className="flex h-screen flex-col">
      <MobileTopBar sidebarOpen={sidebarOpen} onToggleSidebar={() => setSidebarOpen(prev => !prev)} />
      <div className="flex-1 overflow-y-auto">
        <div className="mx-auto max-w-2xl px-4 py-4">
          <SearchBar
            value={filter.searchQuery}
            onChange={setSearchQuery}
            onSubmit={(q) => navigate(`/search?q=${encodeURIComponent(q)}`)}
            autoFocus
          />
          <FilterPills
            selectedChargerType={filter.chargerType}
            onChargerTypeChange={setChargerType}
            selectedAvailability={filter.availability}
            onAvailabilityChange={setAvailability}
          />
          <h2 className="mb-3 mt-4 text-base font-semibold text-neutral-700">{t('search.title')}</h2>
          {filteredStations.length === 0 ? (
            <div className="mt-12 text-center">
              <p className="text-sm text-neutral-400">{t('search.noResults')}</p>
            </div>
          ) : (
            <div className="space-y-3">
              {filteredStations.map(s => (
                <StationCard key={s.id} station={s} onClick={(id) => navigate(`/stations/${id}`)} />
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
