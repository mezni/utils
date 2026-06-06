import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useTranslation } from 'react-i18next'
import MobileTopBar from '../components/MobileTopBar'
import SearchBar from '../components/SearchBar'
import FilterPills from '../components/FilterPills'
import MapPinMarker from '../components/MapPinMarker'
import ZoomControls from '../components/ZoomControls'
import StationCard from '../components/StationCard'
import BottomStationCard from '../components/BottomStationCard'
import { useMockFilter } from '../hooks/useMockFilter'

export default function HomeMapScreen() {
  const { t } = useTranslation()
  const navigate = useNavigate()
  const [sidebarOpen, setSidebarOpen] = useState(true)
  const [selectedStationId, setSelectedStationId] = useState<string | null>(null)
  const { filter, setChargerType, setAvailability, setSearchQuery, filteredStations } = useMockFilter()

  const selectedStation = selectedStationId
    ? filteredStations.find(s => s.id === selectedStationId) ?? null
    : null

  return (
    <div className="flex h-screen flex-col">
      <MobileTopBar
        sidebarOpen={sidebarOpen}
        onToggleSidebar={() => setSidebarOpen(prev => !prev)}
        notificationCount={3}
      />
      <div className="flex flex-1 overflow-hidden">
        <div className="relative flex-1 bg-[#EAF0E6]">
          {filteredStations.map(s => (
            <MapPinMarker
              key={s.id}
              state={selectedStationId === s.id ? 'selected' : s.availability === 'available' ? 'default' : 'unavailable'}
              stationName={s.name}
              hasAvailable={s.availability === 'available'}
              onClick={() => setSelectedStationId(s.id)}
              position={{
                top: `${30 + (filteredStations.indexOf(s) * 3) % 60}%`,
                left: `${15 + (filteredStations.indexOf(s) * 7) % 70}%`,
              }}
            />
          ))}
          <ZoomControls onZoomIn={() => {}} onZoomOut={() => {}} />
          {selectedStation && (
            <div className="absolute bottom-0 left-0 right-0">
              <BottomStationCard
                station={selectedStation}
                onClick={(id) => navigate(`/stations/${id}`)}
              />
            </div>
          )}
        </div>
        {sidebarOpen && (
          <aside className="flex w-80 flex-col border-l border-neutral-200 bg-white rtl:border-r">
            <SearchBar
              value={filter.searchQuery}
              onChange={setSearchQuery}
              onSubmit={(q) => navigate(`/search?q=${encodeURIComponent(q)}`)}
            />
            <FilterPills
              selectedChargerType={filter.chargerType}
              onChargerTypeChange={setChargerType}
              selectedAvailability={filter.availability}
              onAvailabilityChange={setAvailability}
            />
            <div className="flex-1 overflow-y-auto px-4 pb-4">
              {filteredStations.length === 0 ? (
                <p className="mt-8 text-center text-sm text-neutral-400">{t('home.noStations')}</p>
              ) : (
                <div className="space-y-3">
                  {filteredStations.map(s => (
                    <StationCard key={s.id} station={s} onClick={(id) => navigate(`/stations/${id}`)} />
                  ))}
                </div>
              )}
            </div>
          </aside>
        )}
      </div>
    </div>
  )
}
